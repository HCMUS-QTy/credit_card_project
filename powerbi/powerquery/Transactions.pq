let
    // CURRENT is buffered once; this refresh reads one immutable CSV snapshot.
    Root = Text.TrimEnd(DataFolder, {"\", "/"}) & "\",
    CurrentBinary = Binary.Buffer(File.Contents(Root & "CURRENT.json")),
    CurrentRecord = Json.Document(CurrentBinary),
    Current =
        if CurrentRecord[format_version] = 1 then CurrentRecord
        else error "Unsupported Power BI export version. Run export_powerbi.py again.",
    RelativePath = Text.Replace(Current[csv][path], "/", "\"),
    CsvBinary = Binary.Buffer(File.Contents(Root & RelativePath)),
    CsvRows = Csv.Document(
        CsvBinary,
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(CsvRows, [PromoteAllScalars = true]),
    RequiredColumns = {
        "source_snapshot_id", "transaction_id", "User", "Card",
        "date_formatted", "time_formatted", "merchant", "city", "final_status",
        "currency", "amount_numeric", "amount_vnd", "fx_rate"
    },
    Selected = Table.SelectColumns(Headers, RequiredColumns, MissingField.Error),
    TextColumns = Table.TransformColumnTypes(
        Selected,
        List.Transform(RequiredColumns, each {_, type text}),
        "en-US"
    ),
    ToCurrency = (value as nullable text) as nullable number =>
        if value = null or Text.Trim(value) = "" then null
        else Currency.From(value, "en-US"),
    Amounts = Table.TransformColumns(TextColumns, {
        {"amount_numeric", ToCurrency, Currency.Type},
        {"amount_vnd", ToCurrency, Currency.Type},
        {"fx_rate", ToCurrency, Currency.Type}
    }),
    TransactionDate = Table.AddColumn(
        Amounts,
        "TransactionDate",
        each try Date.FromText(
            [date_formatted], [Format = "dd/MM/yyyy", Culture = "vi-VN"]
        ) otherwise null,
        type nullable date
    ),
    TransactionTime = Table.AddColumn(
        TransactionDate,
        "TransactionTime",
        each try Time.FromText(
            [time_formatted], [Format = "HH:mm:ss", Culture = "en-US"]
        ) otherwise null,
        type nullable time
    ),
    DisplayLabels = Table.TransformColumns(TransactionTime, {
        {"merchant", each if _ = null or Text.Trim(_) = ""
            then "UNKNOWN_MERCHANT" else Text.Trim(_), type text},
        {"city", each if _ = null or Text.Trim(_) = ""
            then "UNKNOWN_CITY" else Text.Trim(_), type text}
    }),
    Buffered = Table.Buffer(DisplayLabels),
    SnapshotIds = List.Distinct(Buffered[source_snapshot_id]),
    SnapshotMatches = List.Count(SnapshotIds) = 0 or (
        List.Count(SnapshotIds) = 1 and SnapshotIds{0} = Current[source_snapshot_id]
    ),
    Verified =
        if Table.RowCount(Buffered) <> Current[transaction_count] then
            error "CSV row count does not match CURRENT.json. Keep the export bundle together."
        else if not SnapshotMatches then
            error "CSV source_snapshot_id does not match CURRENT.json."
        else Buffered
in
    Verified
