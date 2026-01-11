# ETL Stream Documentation

Welcome to the documentation for the `etl-stream` project.

## Guide

1.  **[Architecture Overview](architecture.md)**
    *   High-level system design.
    *   Mermaid diagrams.
    *   Technology stack.

2.  **[Data Flow Journey](data_flow.md)**
    *   Step-by-step explanation of how data moves from PostgreSQL to Snowflake.
    *   Explanation of Buffering, Arrow conversion, and Python bridging.

3.  **[Component Reference](components.md)**
    *   Deep dive into Rust modules (`PipelineManager`, `WalMonitor`) and the Python integration.

4.  **[Maintenance & Operations](maintenance.md)**
    *   How to build and run the project.
    *   Configuration guide.
    *   Troubleshooting common issues.
