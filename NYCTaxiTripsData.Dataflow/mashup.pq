[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NYC Yellow Taxi Trips_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "NYC Yellow Taxi Trips_DataDestinationTransform"]]]}]
shared #"NYC Yellow Taxi Trips" = let
  Source = Csv.Document(Web.Contents("https://data.cityofnewyork.us/resource/4b4i-vvec.csv"), [Delimiter = ",", Columns = 19, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"vendorid", Int64.Type}, {"tpep_pickup_datetime", type datetime}, {"tpep_dropoff_datetime", type datetime}, {"passenger_count", Int64.Type}, {"trip_distance", type number}, {"ratecodeid", Int64.Type}, {"store_and_fwd_flag", type text}, {"pulocationid", Int64.Type}, {"dolocationid", Int64.Type}, {"payment_type", Int64.Type}, {"fare_amount", type number}, {"extra", type number}, {"mta_tax", type number}, {"tip_amount", type number}, {"tolls_amount", type number}, {"improvement_surcharge", type number}, {"total_amount", type number}, {"congestion_surcharge", type number}, {"airport_fee", type number}})
in
  #"Changed column type";
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
  Source = Csv.Document(Web.Contents("https://data.cityofnewyork.us/resource/peyi-gg4n.csv"), [Delimiter = ",", Columns = 20, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"vendorid", Int64.Type}, {"lpep_pickup_datetime", type datetime}, {"lpep_dropoff_datetime", type datetime}, {"store_and_fwd_flag", type text}, {"ratecodeid", Int64.Type}, {"pulocationid", Int64.Type}, {"dolocationid", Int64.Type}, {"passenger_count", Int64.Type}, {"trip_distance", type number}, {"fare_amount", type number}, {"extra", type number}, {"mta_tax", type number}, {"tip_amount", type number}, {"tolls_amount", type number}, {"ehail_fee", type text}, {"improvement_surcharge", type number}, {"total_amount", type number}, {"payment_type", Int64.Type}, {"trip_type", Int64.Type}, {"congestion_surcharge", type number}})
in
  #"Changed column type";
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
