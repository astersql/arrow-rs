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

use super::*;
use crate::basic::PageType;
use crate::file::metadata::thrift::{DataPageHeader, DataPageHeaderV2};
use crate::schema::types::SchemaDescriptor;
use std::io::Cursor;
fn column(optional: bool) -> ColumnDescPtr {
    let schema = Arc::new(
        crate::schema::parser::parse_message_type(if optional {
            "message m { OPTIONAL BYTE_ARRAY data; }"
        } else {
            "message m { REQUIRED BYTE_ARRAY data; }"
        })
        .unwrap(),
    );
    SchemaDescriptor::new(schema).column(0)
}
fn header() -> PageHeader {
    PageHeader {
        r#type: PageType::DATA_PAGE,
        uncompressed_page_size: 8,
        compressed_page_size: 8,
        crc: None,
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
        data_page_header: Some(DataPageHeader {
            num_values: 1,
            encoding: Encoding::PLAIN,
            definition_level_encoding: Encoding::RLE,
            repetition_level_encoding: Encoding::RLE,
            statistics: None,
        }),
    }
}
#[test]
fn value_stream_checks_short_extra_and_unconsumed_bytes() {
    for (data, size) in [(vec![1], 2), (vec![1, 2, 3], 2)] {
        let mut source = ValueStream {
            reader: Box::new(Cursor::new(data)),
            remaining: size,
            batch_limit: 1,
        };
        let result = source.read_exact(&mut [0; 2]).and_then(|_| source.finish());
        assert!(result.is_err());
    }
    let mut source = ValueStream {
        reader: Box::new(Cursor::new(vec![1, 2])),
        remaining: 2,
        batch_limit: 1,
    };
    assert!(source.finish().is_err());
    assert!(source.discard(3).is_err());
    source.discard(2).unwrap();
    source.finish().unwrap();
}
#[test]
fn v1_level_prefix_and_v2_region_lengths_are_bounded() {
    let col = column(true);
    let mut bytes = u32::MAX.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0; 4]);
    assert!(
        open(
            Box::new(Cursor::new(bytes)),
            header(),
            Compression::UNCOMPRESSED,
            &col
        )
        .is_err()
    );
    for length in [-1, 9] {
        let mut h = header();
        h.r#type = PageType::DATA_PAGE_V2;
        h.data_page_header = None;
        h.data_page_header_v2 = Some(DataPageHeaderV2 {
            num_values: 1,
            num_rows: 1,
            num_nulls: 0,
            encoding: Encoding::PLAIN,
            definition_levels_byte_length: length,
            repetition_levels_byte_length: 0,
            is_compressed: Some(false),
            statistics: None,
        });
        assert!(
            open(
                Box::new(Cursor::new(vec![0; 8])),
                h,
                Compression::UNCOMPRESSED,
                &col
            )
            .is_err()
        );
    }
}
#[test]
fn v2_uncompressed_values_and_all_null_pages_finish_without_codec() {
    let mut h = header();
    h.r#type = PageType::DATA_PAGE_V2;
    h.data_page_header = None;
    h.compressed_page_size = 5;
    h.uncompressed_page_size = 5;
    h.data_page_header_v2 = Some(DataPageHeaderV2 {
        num_values: 1,
        num_rows: 1,
        num_nulls: 0,
        encoding: Encoding::PLAIN,
        definition_levels_byte_length: 0,
        repetition_levels_byte_length: 0,
        is_compressed: Some(false),
        statistics: None,
    });
    let (_, mut source) = open(
        Box::new(Cursor::new(vec![1, 0, 0, 0, b'a'])),
        h.clone(),
        Compression::GZIP(Default::default()),
        &column(false),
    )
    .unwrap();
    let mut value = [0; 5];
    source.read_exact(&mut value).unwrap();
    assert_eq!(value, [1, 0, 0, 0, b'a']);
    source.finish().unwrap();
    h.compressed_page_size = 2;
    h.uncompressed_page_size = 2;
    let page = h.data_page_header_v2.as_mut().unwrap();
    page.num_nulls = 1;
    page.definition_levels_byte_length = 2;
    let (_, mut source) = open(
        Box::new(Cursor::new(vec![2, 0])),
        h,
        Compression::GZIP(Default::default()),
        &column(true),
    )
    .unwrap();
    source.finish().unwrap();
}
#[cfg(any(feature = "flate2-rust_backend", feature = "flate2-zlib-rs"))]
#[test]
fn compressed_stream_rejects_truncated_frame() {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(vec![], Default::default());
    encoder.write_all(&[0; 8]).unwrap();
    let mut bytes = encoder.finish().unwrap();
    bytes.truncate(bytes.len() - 4);
    let mut h = header();
    h.compressed_page_size = bytes.len() as i32;
    let (_, mut source) = open(
        Box::new(Cursor::new(bytes)),
        h,
        Compression::GZIP(Default::default()),
        &column(false),
    )
    .unwrap();
    let result = source.read_exact(&mut [0; 8]).and_then(|_| source.finish());
    assert!(result.is_err());
}

#[test]
fn eligibility_preserves_small_dictionary_and_nonstreamable_codec_paths() {
    let mut h = header();
    let column = column(false);
    assert!(!eligible(&h, Compression::UNCOMPRESSED, &column));
    h.uncompressed_page_size = (1 << 20) + 1;
    assert!(eligible(&h, Compression::UNCOMPRESSED, &column));
    assert!(!eligible(&h, Compression::SNAPPY, &column));
    assert!(!eligible(&h, Compression::LZ4_RAW, &column));
    h.data_page_header.as_mut().unwrap().encoding = Encoding::RLE_DICTIONARY;
    assert!(!eligible(&h, Compression::UNCOMPRESSED, &column));
    #[cfg(feature = "crc")]
    {
        h.data_page_header.as_mut().unwrap().encoding = Encoding::PLAIN;
        h.crc = Some(0);
        assert!(!eligible(&h, Compression::UNCOMPRESSED, &column));
    }
}
