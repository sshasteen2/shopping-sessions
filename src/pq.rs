use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

const USER_SCHEMA: &str = r#"
message schema {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}
"#;

const CAMP_SCHEMA: &str = r#"
message schema {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}
"#;

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        // IDs are nearly all distinct, so dictionaries only cost time; and
        // nothing reads the statistics.
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build()
}

pub fn write_user_daily(
    path: &Path,
    user_id: Vec<ByteArray>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    top_cat: Vec<Option<ByteArray>>,
) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let file = File::create(path).expect("create parquet");
    let schema = Arc::new(parse_message_type(USER_SCHEMA).unwrap());
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props())).unwrap();
    let mut rg = writer.next_row_group().unwrap();

    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<ByteArrayType>()
        .write_batch(&user_id, None, None)
        .unwrap();
    col.close().unwrap();

    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int32Type>().write_batch(&day, None, None).unwrap();
    col.close().unwrap();

    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&sessions, None, None)
        .unwrap();
    col.close().unwrap();

    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&events, None, None)
        .unwrap();
    col.close().unwrap();

    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&revenue, None, None)
        .unwrap();
    col.close().unwrap();

    let mut present = Vec::new();
    let mut def = Vec::with_capacity(top_cat.len());
    for v in &top_cat {
        match v {
            Some(b) => {
                present.push(b.clone());
                def.push(1);
            }
            None => def.push(0),
        }
    }
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<ByteArrayType>()
        .write_batch(&present, Some(&def), None)
        .unwrap();
    col.close().unwrap();

    rg.close().unwrap();
    writer.close().unwrap();
}

pub fn write_campaign_daily(
    path: &Path,
    campaign_id: Vec<ByteArray>,
    channel: Vec<ByteArray>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    purchasing: Vec<i64>,
    revenue: Vec<i64>,
    assisted: Vec<i64>,
) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let file = File::create(path).expect("create parquet");
    let schema = Arc::new(parse_message_type(CAMP_SCHEMA).unwrap());
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props())).unwrap();
    let mut rg = writer.next_row_group().unwrap();

    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<ByteArrayType>()
        .write_batch(&campaign_id, None, None)
        .unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<ByteArrayType>()
        .write_batch(&channel, None, None)
        .unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int32Type>().write_batch(&day, None, None).unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&sessions, None, None)
        .unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&purchasing, None, None)
        .unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&revenue, None, None)
        .unwrap();
    col.close().unwrap();
    let mut col = rg.next_column().unwrap().unwrap();
    col.typed::<Int64Type>()
        .write_batch(&assisted, None, None)
        .unwrap();
    col.close().unwrap();

    rg.close().unwrap();
    writer.close().unwrap();
}
