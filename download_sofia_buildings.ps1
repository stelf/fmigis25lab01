# download_bulgaria_buildings.ps1
# Скрипт за изтегляне на файловете със сгради за България от Microsoft GlobalMLBuildingFootprints
# Чете dataset-links.csv, филтрира само за Bulgaria, обновява input.txt и тегли .gz файловете в IN/
# Работи на Windows и macOS (pwsh)

# Проверка за PowerShell 7+
$psver = $PSVersionTable.PSVersion.Major
if ($psver -lt 7) {
    Write-Host "This script requires PowerShell 7 or later. Exiting."
    exit 1
}

$datasetUrl = "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
$inputFile = "input.txt"
$outDir = "IN"
$country = "Bulgaria"

if (!(Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir | Out-Null
}

# Download dataset-links.csv
$tmpCsv = Join-Path $env:TEMP "dataset-links.csv"
Invoke-WebRequest -Uri $datasetUrl -OutFile $tmpCsv

# Parse CSV and filter for Bulgaria
$bulgariaRows = Import-Csv $tmpCsv | Where-Object { $_.'Country or Region' -eq $country }

# Update input.txt with Bulgaria rows
$bulgariaRows | ForEach-Object {
    "$($_.QuadKey),$($_.'Country or Region'),$($_.URL)" 
} | Set-Content $inputFile

# Download each file listed in input.txt
$lines = Get-Content $inputFile | Where-Object { $_ -and ($_ -notmatch '^#') }

$downloaded = @()
foreach ($line in $lines) {
    $fields = $line -split ','
    if ($fields.Length -ge 3) {
        $url = $fields[2].Trim()
        $filename = Split-Path $url -Leaf
        $dest = Join-Path $outDir $filename
        if (!(Test-Path $dest)) {
            Write-Host "Downloading $url ..."
            try {
                Invoke-WebRequest -Uri $url -OutFile $dest
                $downloaded += $filename
            } catch {
                Write-Warning "Failed to download $url"
            }
        } else {
            Write-Host "$filename already exists. Skipping."
            $downloaded += $filename
        }
    }
}

# Update input.txt with only successfully downloaded files
if ($downloaded.Count -gt 0) {
    $lines | Where-Object { $downloaded -contains ((($_ -split ',')[2]).Trim() | Split-Path -Leaf) } | Set-Content $inputFile
    Write-Host "Updated input.txt with successfully downloaded files."
}

Remove-Item $tmpCsv -ErrorAction SilentlyContinue