# API URL (för både meta-information och dataanrop)
$apiUrl = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/BE/BE0101/BE0101J/Flyttningar97"

# === Funktion: Hämta alla tillgängliga datum (Tid-variabeln) ===
function Get-AvailableDates {
    param(
        [Parameter(Mandatory=$true)]
        [string]$MetaApiUrl
    )
    try {
        Write-Host "Hämtar meta-information från API:t..."
        $metaResponse = Invoke-RestMethod -Uri $MetaApiUrl -Method Get -ContentType "application/json"
        if ($metaResponse.variables) {
            $tidVar = $metaResponse.variables | Where-Object { $_.code -eq "Tid" }
            if ($tidVar -and $tidVar.values) {
                return $tidVar.values
            }
            else {
                Write-Error "Hittade inte 'Tid'-variabeln i meta-informationen."
                return @()
            }
        }
        else {
            Write-Error "Ingen meta-information hittades."
            return @()
        }
    }
    catch {
        Write-Error "Fel vid hämtning av meta-information: $_"
        return @()
    }
}

# Hämta alla tillgängliga datum via funktionen
$allDates = Get-AvailableDates -MetaApiUrl $apiUrl
if ($allDates.Count -eq 0) {
    Write-Host "Inga datum kunde hämtas. Använder förvalt datumintervall."
    $allDates = @("2018", "2019", "2020", "2021", "2022", "2023", "2024")
}
else {
    Write-Host "Tillgängliga datum: $($allDates -join ', ')"
}

# Bygg payloaden – använd $allDates för 'Tid'
$payload = @{
    query = @(
        @{
            code = "Region";
            selection = @{
                filter = "vs:RegionKommun07EjAggr";
                values = @("1380")
            }
        },
        @{
            code = "Alder";
            selection = @{
                filter = "agg:Ålder5år";
                values = @(
                    "-4",
                    "5-9",
                    "10-14",
                    "15-19",
                    "20-24",
                    "25-29",
                    "30-34",
                    "35-39",
                    "40-44",
                    "45-49",
                    "50-54",
                    "55-59",
                    "60-64",
                    "65-69",
                    "70-74",
                    "75-79",
                    "80-84",
                    "85-89",
                    "90-94",
                    "95-99",
                    "100+"
                )
            }
        },
        @{
            code = "Kon";
            selection = @{
                filter = "item";
                values = @("1", "2")
            }
        },
        @{
            code = "ContentsCode";
            selection = @{
                filter = "item";
                values = @(
                    "BE0101AU",
                    "BE0101AV",
                    "BE0101AX",
                    "BE0101AY",
                    "BE0101AZ",
                    "BE0101A1",
                    "BE0101A2",
                    "BE0101A3"
                )
            }
        },
        @{
            code = "Tid";
            selection = @{
                filter = "item";
                values = $allDates
            }
        }
    );
    response = @{
        format = "json"
    }
} | ConvertTo-Json -Depth 4

# === Funktion: Utför API-anropet med 60 sekunders timeout ===
function Invoke-APIRequest {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Uri,
        [Parameter(Mandatory=$true)]
        [string]$JsonPayload,
        [int]$TimeoutSec = 60
    )

    $client = New-Object System.Net.Http.HttpClient
    $client.Timeout = [TimeSpan]::FromSeconds($TimeoutSec)
    
    try {
        Write-Host "Skickar API-anrop till $Uri..."
        $content = New-Object System.Net.Http.StringContent($JsonPayload, [Text.Encoding]::UTF8, "application/json")
        $response = $client.PostAsync($Uri, $content).Result
        $response.EnsureSuccessStatusCode() | Out-Null
        $responseContent = $response.Content.ReadAsStringAsync().Result
        return $responseContent
    }
    catch {
        Write-Error "Fel vid API-anrop: $_"
    }
    finally {
        $client.Dispose()
    }
}

# Utför API-anropet med timeout 60 sekunder
$responseContent = Invoke-APIRequest -Uri $apiUrl -JsonPayload $payload -TimeoutSec 60
if (-not $responseContent) {
    Write-Error "Inget svar mottogs från API:t."
    exit
}

# Tolka API-svaret (JSON)
try {
    $responseData = $responseContent | ConvertFrom-Json
}
catch {
    Write-Error "Kunde inte tolka API-svaret som JSON. Rådata: $responseContent"
    exit
}

if (-not $responseData.data) {
    Write-Error "Ingen data returnerades från SCB:s API."
    exit
}

# Definiera den önskade ordningen för innehållskoderna (baserat på API-svaret)
$desiredOrder = @(
    @{ code = "BE0101AV"; name = "Utflyttningar"; index = 1 },
    @{ code = "BE0101AY"; name = "Utvandringar";   index = 3 },
    @{ code = "BE0101A3"; name = "Inrikes utflyttningar"; index = 7 },
    @{ code = "BE0101AX"; name = "Invandringar";   index = 2 },
    @{ code = "BE0101A2"; name = "Inrikes inflyttningar"; index = 6 },
    @{ code = "BE0101AZ"; name = "Flyttningsöverskott";   index = 4 },
    @{ code = "BE0101AU"; name = "Inflyttningar";  index = 0 },
    @{ code = "BE0101A1"; name = "Invandringsöverskott"; index = 5 }
)

# Extrahera och mappa ut dataraderna med en bestämd kolumnordning
$dataRows = @()
foreach ($row in $responseData.data) {
    # Anta att $row.key innehåller [Region, Ålder, Kön, År]
    $year   = $row.key[-1]
    $region = $row.key[0]
    $age    = $row.key[1]
    $gender = if ($row.key[2] -eq "1") { "Man" } else { "Kvinna" }
    
    # Skapa ett [ordered] objekt för att försäkra att kolumnerna sparas i rätt ordning
    $obj = [ordered]@{
        "År"     = $year
        "Region" = $region
        "Ålder"  = $age
        "Kön"    = $gender
    }
    foreach ($col in $desiredOrder) {
        $obj[$col.name] = $row.values[$col.index]
    }
    $dataRows += New-Object PSObject -Property $obj
}

# Spara resultatet till en CSV-fil
$outputPath = "SCB_Flyttningsdata.csv"
$dataRows | Export-Csv -Path $outputPath -NoTypeInformation -Encoding UTF8
Write-Host "✅ Data har hämtats och sparats till filen: $outputPath"
