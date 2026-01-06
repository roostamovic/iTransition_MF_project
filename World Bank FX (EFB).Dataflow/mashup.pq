[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "File", TransformationQueryName = "DefaultDestination_DataDestinationTransform"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "FX (EFB)_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "FX (EFB)_DataDestinationTransform"]]]}]
shared #"FX (EFB)" = let
    // 1. Load ECB CSV (DO NOT limit columns)
    Source =
        Csv.Document(
            Web.Contents(
                "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"
            ),
            [
                Delimiter = ",",
                Encoding = 65001,
                QuoteStyle = QuoteStyle.Csv
            ]
        ),

    // 2. Promote headers
    PromoteHeaders =
        Table.PromoteHeaders(Source, [PromoteAllScalars = true]),

    // 3. Rename essential columns
    RenameColumns =
        Table.RenameColumns(
            PromoteHeaders,
            {
                {"TIME_PERIOD", "Date"},
                {"OBS_VALUE", "Rate"}
            }
        ),

    // 4. Convert data types
    ChangeTypes =
        Table.TransformColumnTypes(
            RenameColumns,
            {
                {"Date", type date},
                {"Rate", type number}
            },
            "en-US"
        ),
  #"Changed column type" = Table.TransformColumnTypes(ChangeTypes, {{"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Changed column type", {{"Date", Order.Descending}})
in
    #"Sorted rows";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "c45e0693-30d0-4f7e-955e-3bcca7cd0beb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "78ab0d01-3eeb-4acd-8aaa-f80501acf15e"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "Bronze"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "WorldBankData"]}[Content]
in
  #"Navigation 5";
shared DefaultDestination_DataDestinationTransform = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
shared #"FX (EFB)_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c45e0693-30d0-4f7e-955e-3bcca7cd0beb"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "78ab0d01-3eeb-4acd-8aaa-f80501acf15e"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "Bronze"]}[Content],
  Navigation_5 = Navigation_4{[Name = "WorldBankData"]}[Content],
  FileNavigation = Navigation_5{[Name = "FX_EFB.csv"]}?[Content]?
in
  FileNavigation;
shared #"FX (EFB)_DataDestinationTransform" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
