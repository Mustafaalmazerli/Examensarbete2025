$ApiUrl = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/BE/BE0101/BE0101A/BefolkManadCKM"
$ServerName = "MUSTAF"  
$DatabaseName = "SCBPopulationDB"

#UTF-8 för att säkerställa rätt språk tecken
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

#  ansluta till SQL Server
function Connect-ToSQLServer {
    try {
        $ConnectionString = "Driver={ODBC Driver 17 for SQL Server};Server=$ServerName;Database=$DatabaseName;Trusted_Connection=yes;"
        $SqlConnection = New-Object System.Data.Odbc.OdbcConnection($ConnectionString)
        $SqlConnection.Open()
        Write-Host "Ansluten till SQL Server."
        return $SqlConnection
    } catch {
        Write-Host "Fel vid anslutning till SQL Server: $_"
        exit
    }
}

#  infoga data i SQL-tabellen
function Add-DataToSQL {
  param (
      [System.Data.Odbc.OdbcConnection]$SqlConnection,
      [PSCustomObject]$Data
  )

  # Kontrollera om datan redan finns(byt ut BefolkningData till tabell namn)
  $CheckQuery = @"
  SELECT COUNT(*) FROM [SCBPopulationDB].[dbo].[BefolkningData]
  WHERE [Region] = ? AND [Ålder] = ? AND [Kön] = ? AND [Tid] = ?
"@

  $CheckCommand = $SqlConnection.CreateCommand()
  $CheckCommand.CommandText = $CheckQuery

  #parametrar för kontrollfrågan
  $CheckCommand.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Region))) | Out-Null
  $CheckCommand.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Ålder))) | Out-Null
  $CheckCommand.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Kön))) | Out-Null
  $CheckCommand.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Tid))) | Out-Null

  $Count = $CheckCommand.ExecuteScalar()

  if ($Count -eq 0) {
      # Lägg in skanade data( byt ut tabell namn)
      $Query = @"
      INSERT INTO [SCBPopulationDB].[dbo].[BefolkningData] ([Region], [Ålder], [Kön], [Tid], [Värde])
      VALUES (?, ?, ?, ?, ?)
"@

      $Command = $SqlConnection.CreateCommand()
      $Command.CommandText = $Query

      # parametrar för infogningen
      $Command.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Region))) | Out-Null
      $Command.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Ålder))) | Out-Null
      $Command.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Kön))) | Out-Null
      $Command.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", $Data.Tid))) | Out-Null
      $Command.Parameters.Add((New-Object System.Data.Odbc.OdbcParameter("", [int]$Data.Antal))) | Out-Null

      try {
          $Command.ExecuteNonQuery() | Out-Null
          Write-Host "Rad införd i databasen för region: $($Data.Region), tid: $($Data.Tid)"
      } catch {
          Write-Host "Fel vid insättning av data: $_"
      }
  } else {
      Write-Host "Data finns redan i databasen för Region: $($Data.Region), Tid: $($Data.Tid)."
  }
}

#  JSON-payload
$Payload = @"
{
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:CKM03Kommun",
        "values": ["1380"]
      }
    },
    {
      "code": "Alder",
      "selection": {
        "filter": "vs:CKM035årN",
        "values": ["-4", "5-9", "10-14", "15-19", "20-24",
                   "25-29", "30-34", "35-39", "40-44", "45-49",
                   "50-54", "55-59", "60-64", "65-69", "70-74",
                   "75-79", "80-84", "85-89", "90-94", "95-99", "100+5"]
      }
    },
    {
      "code": "Kon",
      "selection": {
        "filter": "item",
        "values": ["1", "2"]
      }
    }
  ],
  "response": {
    "format": "json"
  }
}
"@

Write-Host "Skickar förfrågan till SCB API..."

try {
    $Response = Invoke-RestMethod -Uri $ApiUrl -Method Post -Body $Payload -ContentType "application/json; charset=utf-8" -TimeoutSec 300
    Write-Host "Data hämtad från API!"

    # Omvandla JSON-data till PowerShell-objekt
    $DataRows = @()
    foreach ($Row in $Response.data) {
        $DataObject = [PSCustomObject]@{
            Region = $Row.key[0]
            Ålder  = $Row.key[1]
            Kön    = if ($Row.key[2] -eq "1") { "män" } else { "kvinnor" }
            Tid    = $Row.key[3]
            Antal  = $Row.values[0]
        }
        $DataRows += $DataObject
    }

    # Infoga data i SQL Server
    if ($DataRows.Count -gt 0) {
        $SqlConnection = Connect-ToSQLServer
        foreach ($Row in $DataRows) {
            Add-DataToSQL -SqlConnection $SqlConnection -Data $Row
        }
        $SqlConnection.Close()
        Write-Host "Data har lagrats i SQL Server."
    } else {
        Write-Host "Ingen data att lagra."
    }

} catch {
    Write-Host "Fel vid hämtning av data från SCB API: $($_.Exception.Message)"
}

# Avsluta skriptet
Write-Host "Körningen avslutad: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
