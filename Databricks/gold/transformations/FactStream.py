import dlt

@dlt.table
def FactStreamStg():
    return (
        spark.readStream.table("spotify_catalog.silver.factstream")
    )

dlt.create_streaming_table(name="factstream")
dlt.create_auto_cdc_flow(
    target = "factstream",
    source="FactStreamStg",
    keys=["stream_id"],
    sequence_by="stream_timestamp",
    stored_as_scd_type=2
)