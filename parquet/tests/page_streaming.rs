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

use parquet::basic::Compression;
use parquet::column::reader::ColumnReader;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::sync::Arc;

fn options(streaming: bool, index: bool) -> parquet::file::serialized_reader::ReadOptions {
    let builder = parquet::file::serialized_reader::ReadOptionsBuilder::new();
    let builder = if index {
        builder.with_page_index()
    } else {
        builder
    };
    builder
        .with_reader_properties(
            parquet::file::properties::ReaderProperties::builder()
                .set_page_streaming_enabled(streaming)
                .build(),
        )
        .build()
}

#[expect(
    clippy::fn_params_excessive_bools,
    reason = "independent binary axes define the fixture matrix"
)]
fn file_bytes(
    version: parquet::file::properties::WriterVersion,
    codec: Compression,
    fixed: bool,
    repeated: bool,
    dictionary: bool,
    multi_page: bool,
) -> Vec<u8> {
    use parquet::data_type::{FixedLenByteArray, FixedLenByteArrayType};
    let physical = if fixed {
        "FIXED_LEN_BYTE_ARRAY(262144)"
    } else {
        "BYTE_ARRAY"
    };
    let repetition = if repeated { "REPEATED" } else { "OPTIONAL" };
    let schema = Arc::new(
        parse_message_type(&format!(
            "message schema {{ {repetition} {physical} data; }}"
        ))
        .unwrap(),
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_writer_version(version)
            .set_compression(codec)
            .set_dictionary_enabled(dictionary)
            .set_data_page_size_limit(if multi_page { 2 << 20 } else { 64 << 20 })
            .set_write_batch_size(8)
            .build(),
    );
    let mut bytes = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut bytes, schema, props).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    let defs: Vec<i16> = (0..40).map(|i| i16::from(i % 7 != 0)).collect();
    let reps: Vec<i16> = (0..40)
        .map(|i| i16::from(!(i % 4 == 0 || defs[i] == 0 || (i > 0 && defs[i - 1] == 0))))
        .collect();
    let values: Vec<ByteArray> = (0..40)
        .filter(|i| defs[*i] == 1)
        .map(|row| {
            let size = if !fixed && row % 5 == 0 { 0 } else { 262144 };
            ByteArray::from((0..size).map(|i| (row + i) as u8).collect::<Vec<_>>())
        })
        .collect();
    if fixed {
        let values: Vec<_> = values.into_iter().map(FixedLenByteArray::from).collect();
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(&values, Some(&defs), repeated.then_some(reps.as_slice()))
            .unwrap();
    } else {
        column
            .typed::<ByteArrayType>()
            .write_batch(&values, Some(&defs), repeated.then_some(reps.as_slice()))
            .unwrap();
    }
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();
    bytes
}

// Collects the real typed column results, including level buffers, so clipped
// streamed batches must remain identical to the whole-page implementation.
#[expect(
    clippy::fn_params_excessive_bools,
    reason = "independent binary axes select the compared reader paths"
)]
fn collect(
    bytes: Vec<u8>,
    streaming: bool,
    index: bool,
    fixed: bool,
    repeated: bool,
    skip: bool,
) -> (Vec<Vec<u8>>, Vec<i16>, Vec<i16>) {
    let reader = SerializedFileReader::new_with_options(
        bytes::Bytes::from(bytes),
        options(streaming, index),
    )
    .unwrap();
    let group = reader.get_row_group(0).unwrap();
    let column = group.get_column_reader(0).unwrap();
    let mut out = vec![];
    let mut definitions = vec![];
    let mut repetitions = vec![];
    macro_rules! read {
        ($reader:expr) => {{
            let mut reader = $reader;
            if skip {
                // Read into a current page before skipping both within it and
                // across later page boundaries.
                reader
                    .read_records(
                        1,
                        Some(&mut vec![]),
                        repeated.then_some(&mut vec![]),
                        &mut vec![],
                    )
                    .unwrap();
                assert_eq!(reader.skip_records(9).unwrap(), 9);
            }
            loop {
                let mut values = vec![];
                let mut defs = vec![];
                let mut reps = vec![];
                let (records, _, levels) = reader
                    .read_records(
                        128,
                        Some(&mut defs),
                        repeated.then_some(&mut reps),
                        &mut values,
                    )
                    .unwrap();
                if records == 0 && levels == 0 {
                    break;
                }
                out.extend(values.into_iter().map(|v| v.data().to_vec()));
                definitions.extend(defs);
                repetitions.extend(reps);
            }
        }};
    }
    match column {
        ColumnReader::ByteArrayColumnReader(reader) if !fixed => read!(reader),
        ColumnReader::FixedLenByteArrayColumnReader(reader) if fixed => read!(reader),
        _ => panic!("wrong physical type"),
    }
    (out, definitions, repetitions)
}

#[test]
fn streamed_v1_v2_codecs_levels_pages_and_skips_match_whole_page() {
    use parquet::file::properties::WriterVersion;
    let mut codecs = vec![Compression::UNCOMPRESSED];
    #[cfg(any(feature = "flate2-zlib-rs", feature = "flate2-rust_backend"))]
    codecs.push(Compression::GZIP(Default::default()));
    #[cfg(feature = "brotli")]
    codecs.push(Compression::BROTLI(Default::default()));
    #[cfg(feature = "zstd")]
    codecs.push(Compression::ZSTD(Default::default()));
    #[cfg(feature = "snap")]
    codecs.push(Compression::SNAPPY);
    #[cfg(feature = "lz4")]
    codecs.push(Compression::LZ4_RAW);
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for codec in codecs.iter().copied() {
            for fixed in [false, true] {
                for repeated in [false, true] {
                    for index in [false, true] {
                        for skip in [false, true] {
                            let bytes = file_bytes(version, codec, fixed, repeated, false, true);
                            assert_eq!(
                                collect(bytes.clone(), true, index, fixed, repeated, skip),
                                collect(bytes, false, index, fixed, repeated, skip),
                                "{version:?}/{codec:?}/fixed={fixed}/repeated={repeated}/index={index}/skip={skip}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn dictionary_fallback_and_row_iterator_keep_values() {
    use parquet::file::properties::WriterVersion;
    for dictionary in [false, true] {
        let bytes = file_bytes(
            WriterVersion::PARQUET_1_0,
            Compression::UNCOMPRESSED,
            false,
            false,
            dictionary,
            false,
        );
        let normal = SerializedFileReader::new_with_options(
            bytes::Bytes::from(bytes.clone()),
            options(false, false),
        )
        .unwrap();
        let stream =
            SerializedFileReader::new_with_options(bytes::Bytes::from(bytes), options(true, false))
                .unwrap();
        let expected = normal
            .get_row_iter(None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let actual = stream
            .get_row_iter(None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn malformed_plain_length_is_rejected_without_allocating_declared_value() {
    use parquet::file::properties::WriterVersion;
    let bytes = file_bytes(
        WriterVersion::PARQUET_1_0,
        Compression::UNCOMPRESSED,
        false,
        false,
        false,
        false,
    );
    // First non-null value has length 262144 and bytes 1,2,3,4...
    let pattern = [0, 0, 4, 0, 1, 2, 3, 4, 5, 6];
    let offset = bytes
        .windows(pattern.len())
        .position(|b| b == pattern)
        .unwrap();
    for prefix in [(-1i32).to_le_bytes(), i32::MAX.to_le_bytes()] {
        let mut corrupt = bytes.clone();
        corrupt[offset..offset + 4].copy_from_slice(&prefix);
        let reader = SerializedFileReader::new_with_options(
            bytes::Bytes::from(corrupt),
            options(true, false),
        )
        .unwrap();
        let group = reader.get_row_group(0).unwrap();
        let ColumnReader::ByteArrayColumnReader(mut column) = group.get_column_reader(0).unwrap()
        else {
            panic!()
        };
        assert!(
            column
                .read_records(128, Some(&mut vec![]), None, &mut vec![])
                .is_err()
        );
    }
}

#[test]
fn streaming_clips_large_requested_batches_and_preserves_all_rows() {
    let bytes = file_bytes(
        parquet::file::properties::WriterVersion::PARQUET_1_0,
        Compression::UNCOMPRESSED,
        true,
        false,
        false,
        false,
    );
    let reader =
        SerializedFileReader::new_with_options(bytes::Bytes::from(bytes), options(true, false))
            .unwrap();
    let group = reader.get_row_group(0).unwrap();
    let ColumnReader::FixedLenByteArrayColumnReader(mut column) =
        group.get_column_reader(0).unwrap()
    else {
        panic!()
    };
    let mut total = 0;
    loop {
        let mut values = vec![];
        let mut levels = vec![];
        let (rows, _, _) = column
            .read_records(128, Some(&mut levels), None, &mut values)
            .unwrap();
        if rows == 0 {
            break;
        }
        assert!(rows <= 5, "unbounded streamed batch of {rows} rows");
        total += rows;
    }
    assert_eq!(total, 40);
}

#[test]
fn interleaved_file_columns_do_not_share_stream_seek_position() {
    let mut file = tempfile::tempfile().unwrap();
    let schema = Arc::new(
        parse_message_type("message m { REQUIRED BYTE_ARRAY a; REQUIRED BYTE_ARRAY b; }").unwrap(),
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(64 << 20)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(&mut file, schema, props).unwrap();
    let mut group = writer.next_row_group().unwrap();
    for c in 0..2 {
        let mut column = group.next_column().unwrap().unwrap();
        let values: Vec<_> = (0..8)
            .map(|row| ByteArray::from(vec![(c * 10 + row) as u8; 262144]))
            .collect();
        column
            .typed::<ByteArrayType>()
            .write_batch(&values, None, None)
            .unwrap();
        column.close().unwrap();
    }
    group.close().unwrap();
    writer.close().unwrap();
    let reader = SerializedFileReader::new_with_options(file, options(true, true)).unwrap();
    let group = reader.get_row_group(0).unwrap();
    let ColumnReader::ByteArrayColumnReader(mut a) = group.get_column_reader(0).unwrap() else {
        panic!()
    };
    let ColumnReader::ByteArrayColumnReader(mut b) = group.get_column_reader(1).unwrap() else {
        panic!()
    };
    for row in 0..8 {
        for (c, column) in [(0, &mut a), (1, &mut b)] {
            let mut values = vec![];
            assert_eq!(
                column.read_records(1, None, None, &mut values).unwrap(),
                (1, 1, 1)
            );
            assert!(values[0].data().iter().all(|v| *v == (c * 10 + row) as u8));
        }
    }
}
