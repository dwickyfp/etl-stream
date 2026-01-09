//! Destination handler enum for supporting multiple destination types.

pub mod snowflake_destination;

use etl::destination::Destination;
use etl::error::EtlResult;
use etl::types::{Event, TableId, TableRow};

pub use snowflake_destination::SnowflakeDestination;

/// Enum wrapper for different destination types.
///
/// This allows the pipeline to work with different destination implementations
/// while maintaining type safety and avoiding trait object overhead.
#[derive(Debug, Clone)]
pub enum DestinationHandler {
    Snowflake(SnowflakeDestination),
}

impl Destination for DestinationHandler {
    fn name() -> &'static str {
        "destination_handler"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        match self {
            DestinationHandler::Snowflake(dest) => dest.truncate_table(table_id).await,
        }
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        match self {
            DestinationHandler::Snowflake(dest) => dest.write_table_rows(table_id, rows).await,
        }
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        match self {
            DestinationHandler::Snowflake(dest) => dest.write_events(events).await,
        }
    }
}
