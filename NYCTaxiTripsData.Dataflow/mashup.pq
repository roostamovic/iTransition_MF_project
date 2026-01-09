[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NYC Yellow Taxi Trips_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "NYC Yellow Taxi Trips_DataDestinationTransform"]]]}]
shared #"NYC Yellow Taxi Trips" = //     SourceUrls = {
//         // "https://data.cityofnewyork.us/resource/f9tw-8p66.csv",
//         // "https://data.cityofnewyork.us/resource/74wj-s5ij.csv",
//         // "https://data.cityofnewyork.us/resource/uwyp-dntv.csv",
//         // "https://data.cityofnewyork.us/resource/kerk-3eby.csv",
//         // "https://data.cityofnewyork.us/resource/t7ny-aygi.csv",
//         // "https://data.cityofnewyork.us/resource/gkne-dk5s.csv",
//         // "https://data.cityofnewyork.us/resource/2yzn-sicd.csv",
//         // "https://data.cityofnewyork.us/resource/uacg-pexx.csv",
//         // "https://data.cityofnewyork.us/resource/biws-g3hs.csv",
//         // "https://data.cityofnewyork.us/resource/t29m-gskq.csv",
//         "https://data.cityofnewyork.us/resource/2upf-qytp.csv",
//         "https://data.cityofnewyork.us/resource/kxp8-n2sj.csv",
//         "https://data.cityofnewyork.us/resource/m6nq-qud6.csv",
//         "https://data.cityofnewyork.us/resource/qp3b-zxtp.csv",
//         "https://data.cityofnewyork.us/resource/4b4i-vvec.csv"}

let
    // 1️⃣ List of Parquet URLs
    BaseUrl = "https://d37ci6vzurychx.cloudfront.net",
    Files = {
        "trip-data/yellow_tripdata_2024-01.parquet",
        "trip-data/yellow_tripdata_2024-02.parquet",
        "trip-data/yellow_tripdata_2024-03.parquet",
        "trip-data/yellow_tripdata_2024-04.parquet",
        "trip-data/yellow_tripdata_2024-05.parquet",
        "trip-data/yellow_tripdata_2024-06.parquet",
        "trip-data/yellow_tripdata_2024-07.parquet",
        "trip-data/yellow_tripdata_2024-08.parquet",
        "trip-data/yellow_tripdata_2024-09.parquet",
        "trip-data/yellow_tripdata_2024-10.parquet",
        "trip-data/yellow_tripdata_2024-11.parquet",
        "trip-data/yellow_tripdata_2024-12.parquet",

        "trip-data/yellow_tripdata_2025-01.parquet",
        "trip-data/yellow_tripdata_2025-02.parquet",
        "trip-data/yellow_tripdata_2025-03.parquet",
        "trip-data/yellow_tripdata_2025-04.parquet",
        "trip-data/yellow_tripdata_2025-05.parquet",
        "trip-data/yellow_tripdata_2025-06.parquet",
        "trip-data/yellow_tripdata_2025-07.parquet",
        "trip-data/yellow_tripdata_2025-08.parquet",
        "trip-data/yellow_tripdata_2025-09.parquet",
        "trip-data/yellow_tripdata_2025-10.parquet",
        "trip-data/yellow_tripdata_2025-11.parquet"
    },

    // 2️⃣ Function to load ONE Parquet file safely
    LoadParquet = (path as text) as table =>
        let
            Source =
                Parquet.Document(
                    Binary.Buffer(
                        Web.Contents(
                            BaseUrl,
                            [ RelativePath = path ]
                        )
                    )
                )
        in
            Source,

    // 3️⃣ Load all Parquet files
    Tables = List.Transform(Files, each LoadParquet(_)),

    // 4️⃣ Combine into one table
    Combined = Table.Combine(Tables),

    Typed =
        Table.TransformColumnTypes(
            Combined,
            {
                {"tpep_pickup_datetime", type datetime},
                {"tpep_dropoff_datetime", type datetime},
                {"passenger_count", Int64.Type},
                {"trip_distance", type number},
                {"fare_amount", type number},
                {"total_amount", type number}
            },
            "en-US"
        )


in
    Typed;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "c45e0693-30d0-4f7e-955e-3bcca7cd0beb"]}[Data]{[lakehouseId = "78ab0d01-3eeb-4acd-8aaa-f80501acf15e"]}[Data];
shared #"NYC Yellow Taxi Trips_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c45e0693-30d0-4f7e-955e-3bcca7cd0beb"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "78ab0d01-3eeb-4acd-8aaa-f80501acf15e"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "Bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "NYCTaxiData"]}[Content],
  FileNavigation = Navigation_5{[Name = "nyc_yellow.csv"]}?[Content]?
in
  FileNavigation;
shared #"NYC Yellow Taxi Trips_DataDestinationTransform" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NYC Green Taxi Trips_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "NYC Green Taxi Trips_DataDestinationTransform"]]]}]
shared #"NYC Green Taxi Trips" = let
    // 1️⃣ List of Green Taxi Parquet URLs
    BaseUrl = "https://d37ci6vzurychx.cloudfront.net",
    Files = {
        "trip-data/green_tripdata_2024-01.parquet",
        "trip-data/green_tripdata_2024-02.parquet",
        "trip-data/green_tripdata_2024-03.parquet",
        "trip-data/green_tripdata_2024-04.parquet",
        "trip-data/green_tripdata_2024-05.parquet",
        "trip-data/green_tripdata_2024-06.parquet",
        "trip-data/green_tripdata_2024-07.parquet",
        "trip-data/green_tripdata_2024-08.parquet",
        "trip-data/green_tripdata_2024-09.parquet",
        "trip-data/green_tripdata_2024-10.parquet",
        "trip-data/green_tripdata_2024-11.parquet",
        "trip-data/green_tripdata_2024-12.parquet",

        "trip-data/green_tripdata_2025-01.parquet",
        "trip-data/green_tripdata_2025-02.parquet",
        "trip-data/green_tripdata_2025-03.parquet",
        "trip-data/green_tripdata_2025-04.parquet",
        "trip-data/green_tripdata_2025-05.parquet",
        "trip-data/green_tripdata_2025-06.parquet",
        "trip-data/green_tripdata_2025-07.parquet",
        "trip-data/green_tripdata_2025-08.parquet",
        "trip-data/green_tripdata_2025-09.parquet",
        "trip-data/green_tripdata_2025-10.parquet",
        "trip-data/green_tripdata_2025-11.parquet"
    },

    // 2️⃣ Function to load ONE Parquet file
    LoadParquet = (path as text) as table =>
        let
            Source =
                Parquet.Document(
                    Binary.Buffer(
                        Web.Contents(
                            BaseUrl,
                            [ RelativePath = path ]
                        )
                    )
                )
        in
            Source,

    // 3️⃣ Load all Parquet files
    Tables = List.Transform(Files, each LoadParquet(_)),

    // 4️⃣ Combine all months
    Combined = Table.Combine(Tables),

    Typed =
        Table.TransformColumnTypes(
            Combined,
            {
                {"lpep_pickup_datetime", type datetime},
                {"lpep_dropoff_datetime", type datetime},
                {"passenger_count", Int64.Type},
                {"trip_distance", type number},
                {"fare_amount", type number},
                {"total_amount", type number},
                {"trip_type", Int64.Type}
            },
            "en-US"
        )

in
    Typed;
shared #"NYC Green Taxi Trips_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c45e0693-30d0-4f7e-955e-3bcca7cd0beb"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "78ab0d01-3eeb-4acd-8aaa-f80501acf15e"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "Bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "NYCTaxiData"]}[Content],
  FileNavigation = Navigation_5{[Name = "nyc_green.csv"]}?[Content]?
in
  FileNavigation;
shared #"NYC Green Taxi Trips_DataDestinationTransform" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
