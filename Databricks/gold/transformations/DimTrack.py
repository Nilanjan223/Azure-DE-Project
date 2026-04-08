import dlt

@dlt.table
def DimTrackStg():
    return (
        spark.readStream.table("spotify_catalog.silver.dimtrack")
    )

dlt.create_streaming_table(name="dimtrack")
dlt.create_auto_cdc_flow(
    target = "dimtrack",
    source="DimTrackStg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)