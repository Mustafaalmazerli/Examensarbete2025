# Konfiguration
$ApiUrl = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/BE/BE0101/BE0101G/MBefStatRegionCKM"
$ServerName = "MUSTAF"  
$DatabaseName = "SCBPopulationDB"
$TableName = "BefolkningStats"

$ConnectionString = "Server=$ServerName;Database=$DatabaseName;Trusted_Connection=yes;"

# Ställ in UTF-8 för korrekt teckenhantering
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# spara data i SQL Server
function Add-DataToSQL {
    param (
        [Parameter(Mandatory=$true)]
        [System.Data.SqlClient.SqlConnection]$SqlConnection,
        [Parameter(Mandatory=$true)]
        [PSCustomObject]$Data
    )
    # SQL INSERT-sats
    $Query = @"
    INSERT INTO $TableName (Region, Tid, [100], [110], [115], [130], [135], [140], [150], [155], [175], [179], [220], [230], [235], [260], [270])
    VALUES ('$($Data.Region)', '$($Data.Tid)', '$($Data."100")', '$($Data."110")', '$($Data."115")', '$($Data."130")',
            '$($Data."135")', '$($Data."140")', '$($Data."150")', '$($Data."155")', '$($Data."175")', '$($Data."179")',
            '$($Data."220")', '$($Data."230")', '$($Data."235")', '$($Data."260")', '$($Data."270")')
"@
    $Command = $SqlConnection.CreateCommand()
    $Command.CommandText = $Query
    $Command.ExecuteNonQuery()
}

# Payload för API-förfrågan
$Payload = @{
    query = @(
        @{
            code = "Region"
            selection = @{
                filter = "vs:CKM03Kommun"
                values = @("1380")
            }
        },
        @{
            code = "Forandringar"
            selection = @{
                filter = "item"
                values = @("100", "110", "115", "130", "135", "140", "150", 
                            "155", "175", "179", "220", "230", "235", "260", "270")
            }
        }
    )
    response = @{
        format = "px"
    }
}

# Konvertera Payload till JSON
$PayloadJson = $Payload | ConvertTo-Json -Depth 10
Write-Host "Payload som skickas:" $PayloadJson

# Skicka POST-förfrågan till SCB API
try {
    $Response = Invoke-RestMethod -Uri $ApiUrl -Method POST -Body $PayloadJson -ContentType "application/json"
} catch {
    Write-Host "Fel vid hämtning av data från SCB API: $_"
    exit
}

if ($Response) {
    Write-Host "Data hämtad från SCB API!"

    # Konvertera API-svaret till en sträng
    $ResponseString = $Response.ToString()

    # Försök extrahera tidsvärdet från metadata med regex.
  
    $Tid = ""
    if ($ResponseString -match 'VALUES\("månad"\)="(\d+M\d+)"') {
        $Tid = $matches[1]  
        Write-Host "Extraherad tidsperiod: $Tid"
    } else {
        Write-Host "Ingen tidsperiod hittades i svaret. Avbryter scriptet."
        exit
    }

    # Hitta positionen där DATA börjar 
    $DataStartIndex = $ResponseString.IndexOf("DATA=")
    if ($DataStartIndex -gt -1) {
    
        $DataString = $ResponseString.Substring($DataStartIndex + 5).Trim()
        $DataString = $DataString.TrimEnd(";")
        $DataValues = $DataString -split "\r\n" | Where-Object { $_ -ne "" }

        # Förväntade förändringstyper 
        $ChangeTypes = @("100", "110", "115", "130", "135", "140", "150", "155", "175", "179", "220", "230", "235", "260", "270")

        if ($DataValues.Length -eq $ChangeTypes.Length) {
            # Skapa ett objekt för att lagra data
            $ProcessedData = [PSCustomObject]@{
                Region = "1380"
                Tid = $Tid
                "100" = $DataValues[0]
                "110" = $DataValues[1]
                "115" = $DataValues[2]
                "130" = $DataValues[3]
                "135" = $DataValues[4]
                "140" = $DataValues[5]
                "150" = $DataValues[6]
                "155" = $DataValues[7]
                "175" = $DataValues[8]
                "179" = $DataValues[9]
                "220" = $DataValues[10]
                "230" = $DataValues[11]
                "235" = $DataValues[12]
                "260" = $DataValues[13]
                "270" = $DataValues[14]
            }

            # Anslut till SQL Server
            $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
            try {
                $SqlConnection.Open()
                Write-Host "Ansluten till SQL Server."
            } catch {
                Write-Host "Fel vid anslutning till SQL Server: $_"
                exit
            }

            # Kontrollera om data för denna tidsperiod redan finns
            $CheckQuery = "SELECT COUNT(*) FROM $TableName WHERE Tid = '$Tid'"
            $Command = $SqlConnection.CreateCommand()
            $Command.CommandText = $CheckQuery
            $Exists = $Command.ExecuteScalar()

            if ($Exists -eq 0) {
                # Spara data i SQL Server
                Add-DataToSQL -SqlConnection $SqlConnection -Data $ProcessedData
                Write-Host "Data för månad $Tid har lagrats i SQL Server."
            } else {
                Write-Host "Data för $Tid finns redan i databasen. Ingen insättning gjordes."
            }

            # Stäng SQL-anslutningen
            $SqlConnection.Close()
        } else {
            Write-Host "Antalet extraherade värden ($($DataValues.Length)) matchar inte antalet förväntade förändringstyper ($($ChangeTypes.Length))."
        }
    } else {
        Write-Host "Ingen DATA-del hittades i svaret."
    }
} else {
    Write-Host "Fel vid hämtning av data från SCB API."
}
