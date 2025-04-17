
$apiUrl      = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/BE/BE0101/BE0101J/Flyttningar97"
$ServerName  = "MUSTAF"  
$DatabaseName= "SCBPopulationDB"
$TableName   = "Flyttningsdata" 

$ConnectionString = "Server=$ServerName;Database=$DatabaseName;Trusted_Connection=yes;"

# Ställ in UTF-8 för korrekt teckenhantering
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8



#  Hämta alla tillgängliga datum 
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

#  Utför API-anrop med 60 timeout
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

# Spara en datarad i SQL Server
function Add-FlyttningsDataToSQL {
    param (
        [Parameter(Mandatory=$true)]
        [System.Data.SqlClient.SqlConnection]$SqlConnection,
        [Parameter(Mandatory=$true)]
        [PSCustomObject]$Data
    )
 

    $query = @"
    INSERT INTO $TableName (Tid, Region, Ålder, Kön, Inflyttningar, Utflyttningar, Invandringar, Utvandringar, Flyttningsöverskott, Invandringsöverskott, InrikesInflyttningar, InrikesUtflyttningar)
    VALUES (
        $($Data.År),
        $($Data.Region),
        '$($Data.Ålder)',
        '$($Data.Kön)',
        $($Data.Inflyttningar),
        $($Data.Utflyttningar),
        $($Data.Invandringar),
        $($Data.Utvandringar),
        $($Data.'Flyttningsöverskott'),
        $($Data.Invandringsöverskott),
        $($Data.'Inrikes inflyttningar'),
        $($Data.'Inrikes utflyttningar')
    )
"@
    $Command = $SqlConnection.CreateCommand()
    $Command.CommandText = $query
    $Command.ExecuteNonQuery() | Out-Null
}

# Hämta tillgängliga datum via meta-information

$allDates = Get-AvailableDates -MetaApiUrl $apiUrl
if ($allDates.Count -eq 0) {
    Write-Host "Inga datum kunde hämtas. Använder förvalt datumintervall."
    $allDates = @("1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009",
                  "2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024","2025","2026","2027")
}
else {
    Write-Host "Tillgängliga datum: $($allDates -join ', ')"
}


#  payload för API-förfrågan

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


# API-anropet

$responseContent = Invoke-APIRequest -Uri $apiUrl -JsonPayload $payload -TimeoutSec 60
if (-not $responseContent) {
    Write-Error "Inget svar mottogs från API:t."
    exit
}

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




#sätta in
$desiredOrder = @(
    @{ code = "BE0101AV"; name = "Utflyttningar";           index = 1 },
    @{ code = "BE0101AY"; name = "Utvandringar";            index = 3 },
    @{ code = "BE0101A3"; name = "Inrikes utflyttningar";   index = 7 },
    @{ code = "BE0101AX"; name = "Invandringar";            index = 2 },
    @{ code = "BE0101A2"; name = "Inrikes inflyttningar";   index = 6 },
    @{ code = "BE0101AZ"; name = "Flyttningsöverskott";     index = 4 },
    @{ code = "BE0101AU"; name = "Inflyttningar";           index = 0 },
    @{ code = "BE0101A1"; name = "Invandringsöverskott";    index = 5 }
)


#mappa ut dataraderna

$dataRows = @()
foreach ($row in $responseData.data) {
    
    $year   = $row.key[-1]
    $region = $row.key[0]
    $age    = $row.key[1]
    $gender = if ($row.key[2] -eq "1") { "Man" } else { "Kvinna" }
    

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


# Anslut till SQL
$SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
try {
    $SqlConnection.Open()
    Write-Host "Ansluten till SQL Server."
} catch {
    Write-Error "Fel vid anslutning till SQL Server: $_"
    exit
}

# För varje rad kontrolleras om en post med samma unika nyckel
foreach ($dataRow in $dataRows) {
    $checkQuery = "SELECT COUNT(*) FROM $TableName WHERE Tid = $($dataRow.År) AND Region = $($dataRow.Region) AND Ålder = '$($dataRow.Ålder)' AND Kön = '$($dataRow.Kön)'"
    $command = $SqlConnection.CreateCommand()
    $command.CommandText = $checkQuery
    $exists = $command.ExecuteScalar()
    
    if ($exists -eq 0) {
        Add-FlyttningsDataToSQL -SqlConnection $SqlConnection -Data $dataRow
        Write-Host "Data för år $($dataRow.År), Region $($dataRow.Region), Ålder $($dataRow.Ålder), Kön $($dataRow.Kön) har lagrats i SQL Server."
    }
    else {
        Write-Host "Data för år $($dataRow.År), Region $($dataRow.Region), Ålder $($dataRow.Ålder), Kön $($dataRow.Kön) finns redan. Ingen insättning gjordes."
    }
}

$SqlConnection.Close()
Write-Host " Samtliga data har bearbetats och SQL-anslutningen stängdes."
