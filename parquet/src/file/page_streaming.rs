// Copyright 2026 AsterSQL.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Incremental PLAIN byte-array pages. Levels retain the existing decoders.
use crate::basic::{Compression, Encoding, Type};
use crate::column::page::Page;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::thrift::PageHeader;
use crate::file::reader::ChunkReader;
use crate::schema::types::ColumnDescPtr;
use bytes::Bytes;
use std::io::{BufReader, Read};
use std::sync::Arc;

/// Uncompressed value bytes limited to one page, owned by its column decoder.
pub struct ValueStream {
    reader: Box<dyn Read + Send>,
    pub(crate) remaining: usize,
    pub(crate) batch_limit: usize,
}
impl ValueStream {
    pub(crate) fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        if bytes.len() > self.remaining {
            return Err(general_err!("PLAIN value exceeds remaining page bytes"));
        }
        self.reader.read_exact(bytes)?;
        self.remaining -= bytes.len();
        Ok(())
    }
    pub(crate) fn discard(&mut self, mut n: usize) -> Result<()> {
        if n > self.remaining {
            return Err(general_err!("PLAIN value exceeds remaining page bytes"));
        }
        let mut buffer = [0u8; 1024];
        while n > 0 {
            let take = n.min(buffer.len());
            self.read_exact(&mut buffer[..take])?;
            n -= take;
        }
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> Result<()> {
        if self.remaining != 0 {
            return Err(general_err!(
                "PLAIN page has {} unconsumed bytes",
                self.remaining
            ));
        }
        let mut byte = [0];
        if self.reader.read(&mut byte)? != 0 {
            return Err(general_err!("Page exceeds declared uncompressed size"));
        }
        Ok(())
    }
}
fn codec_reader(
    reader: Box<dyn Read + Send>,
    codec: Compression,
    compressed: bool,
) -> Result<Box<dyn Read + Send>> {
    if !compressed {
        return Ok(reader);
    }
    match codec {
        Compression::UNCOMPRESSED => Ok(reader),
        #[cfg(any(feature = "flate2-zlib-rs", feature = "flate2-rust_backend"))]
        Compression::GZIP(_) => Ok(Box::new(flate2::read::MultiGzDecoder::new(reader))),
        #[cfg(feature = "brotli")]
        Compression::BROTLI(_) => Ok(Box::new(brotli::Decompressor::new(reader, 1024))),
        #[cfg(feature = "zstd")]
        Compression::ZSTD(_) => Ok(Box::new(zstd::stream::read::Decoder::new(reader)?)),
        _ => Err(general_err!("Codec is not streamable")),
    }
}
pub(crate) fn eligible(header: &PageHeader, codec: Compression, column: &ColumnDescPtr) -> bool {
    let encoding = header
        .data_page_header
        .as_ref()
        .map(|h| h.encoding)
        .or_else(|| header.data_page_header_v2.as_ref().map(|h| h.encoding));
    // Preserve CRC validation on the existing whole-page path.
    #[cfg(feature = "crc")]
    if header.crc.is_some() {
        return false;
    }
    encoding == Some(Encoding::PLAIN)
        && header.uncompressed_page_size > 1 << 20
        && matches!(
            column.physical_type(),
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY
        )
        && matches!(
            codec,
            Compression::UNCOMPRESSED
                | Compression::GZIP(_)
                | Compression::BROTLI(_)
                | Compression::ZSTD(_)
        )
}
pub(crate) fn open(
    reader: Box<dyn Read + Send>,
    header: PageHeader,
    codec: Compression,
    column: &ColumnDescPtr,
) -> Result<(Page, ValueStream)> {
    let uncompressed = usize::try_from(header.uncompressed_page_size)?;
    let compressed = usize::try_from(header.compressed_page_size)?;
    let raw: Box<dyn Read + Send> = Box::new(BufReader::with_capacity(
        1024,
        reader.take(compressed as u64),
    ));
    let num_values;
    let mut levels = vec![];
    let mut source;
    if let Some(h) = header.data_page_header.as_ref() {
        num_values = usize::try_from(h.num_values)?;
        source = ValueStream {
            reader: codec_reader(raw, codec, true)?,
            remaining: uncompressed,
            batch_limit: 1,
        };
        for (max_level, encoding) in [
            (column.max_rep_level(), h.repetition_level_encoding),
            (column.max_def_level(), h.definition_level_encoding),
        ] {
            if max_level == 0 {
                continue;
            }
            let length = match encoding {
                Encoding::RLE => {
                    let mut prefix = [0; 4];
                    source.read_exact(&mut prefix)?;
                    levels.extend_from_slice(&prefix);
                    u32::from_le_bytes(prefix) as usize
                }
                #[expect(deprecated, reason = "legacy V1 level encoding remains supported")]
                Encoding::BIT_PACKED => {
                    let bits = 16 - (max_level as u16).leading_zeros() as usize;
                    num_values
                        .checked_mul(bits)
                        .ok_or_else(|| general_err!("level count overflow"))?
                        .div_ceil(8)
                }
                _ => return Err(general_err!("Unsupported V1 level encoding for streaming")),
            };
            if length > source.remaining {
                return Err(general_err!("Level region exceeds page size"));
            }
            let start = levels.len();
            levels.resize(start + length, 0);
            source.read_exact(&mut levels[start..])?;
        }
    } else if let Some(h) = header.data_page_header_v2.as_ref() {
        num_values = usize::try_from(h.num_values)?;
        let nulls = usize::try_from(h.num_nulls)?;
        let _rows = usize::try_from(h.num_rows)?;
        if nulls > num_values {
            return Err(general_err!("More nulls than values in V2 page"));
        }
        let length = usize::try_from(h.repetition_levels_byte_length)?
            .checked_add(usize::try_from(h.definition_levels_byte_length)?)
            .ok_or_else(|| general_err!("level size overflow"))?;
        if length > compressed || length > uncompressed {
            return Err(general_err!("Level region exceeds page size"));
        }
        let mut raw = raw;
        levels.resize(length, 0);
        raw.read_exact(&mut levels)?;
        source = ValueStream {
            reader: codec_reader(raw, codec, h.is_compressed.unwrap_or(true))?,
            remaining: uncompressed - length,
            batch_limit: 1,
        };
    } else {
        return Err(general_err!("Not a data page"));
    }
    let average = source.remaining.checked_div(num_values).unwrap_or(0);
    source.batch_limit = (1usize << 20)
        .checked_div(average)
        .map(|n| n.max(1))
        .unwrap_or(usize::MAX);
    let page = crate::file::serialized_reader::decode_page(
        header,
        Bytes::from(levels),
        column.physical_type(),
        None,
    )?;
    Ok((page, source))
}

// ChunkReader::get_read may share the seek position with other column readers.
// Read bounded, independently addressed ranges instead of retaining such a reader
// while another column or page index can reposition its underlying file.
pub(crate) struct RangeReader<R: ChunkReader> {
    reader: Arc<R>,
    offset: u64,
    remaining: usize,
    buffer: Bytes,
}
impl<R: ChunkReader> RangeReader<R> {
    pub(crate) fn new(reader: Arc<R>, offset: u64, remaining: usize) -> Self {
        Self {
            reader,
            offset,
            remaining,
            buffer: Bytes::new(),
        }
    }
}
impl<R: ChunkReader> Read for RangeReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.buffer.is_empty() && self.remaining != 0 {
            let size = self.remaining.min(64 << 10);
            self.buffer = self
                .reader
                .get_bytes(self.offset, size)
                .map_err(std::io::Error::other)?;
            if self.buffer.len() != size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Truncated page range",
                ));
            }
            self.offset += size as u64;
            self.remaining -= size;
        }
        let n = out.len().min(self.buffer.len());
        out[..n].copy_from_slice(&self.buffer[..n]);
        self.buffer = self.buffer.slice(n..);
        Ok(n)
    }
}

#[cfg(test)]
#[path = "page_streaming_test.rs"]
mod tests;
