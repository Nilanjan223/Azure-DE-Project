import dlt

@dlt.table
def DimArtistStg():
    return (
        spark.readStream.table("spotify_catalog.silver.dimartist")
    )

dlt.create_streaming_table(name="dimartist")
dlt.create_auto_cdc_flow(
    target = "dimartist",
    source="DimArtistStg",
    keys=["artist_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)