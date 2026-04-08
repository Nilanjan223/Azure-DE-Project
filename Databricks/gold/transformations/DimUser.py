import dlt

@dlt.table
def DimUserStg():
    return (
        spark.readStream.table("spotify_catalog.silver.dimuser")
    )

dlt.create_streaming_table(name="dimuser")
dlt.create_auto_cdc_flow(
    target = "dimuser",
    source="DimUserStg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)