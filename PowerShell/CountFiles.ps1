# Count files in directories recursivelly

[CmdletBinding()]
param (
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Diretorio
)

# (Opcional) Ajuste de codificação para acentos no Windows PowerShell 5.x
try {
    if ($PSVersionTable.PSEdition -eq 'Desktop') {
        [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    }
} catch { }

# Helper: caminho relativo sem depender de Uri nem Path.GetRelativePath (PS 5.1-friendly)
function Get-RelativePath {
    param(
        [Parameter(Mandatory = $true)][string]$BasePath,
        [Parameter(Mandatory = $true)][string]$TargetPath
    )

    $ds  = [IO.Path]::DirectorySeparatorChar
    $ads = [IO.Path]::AltDirectorySeparatorChar

    $baseFull   = [IO.Path]::GetFullPath($BasePath).TrimEnd($ds, $ads)
    $targetFull = [IO.Path]::GetFullPath($TargetPath).TrimEnd($ds, $ads)

    # Comparação case-insensitive no Windows, case-sensitive no restante
    $isWindows  = $env:OS -like '*Windows*'
    $cmp        = if ($isWindows) { [System.StringComparison]::OrdinalIgnoreCase }
                  else            { [System.StringComparison]::Ordinal }

    if ([string]::Equals($baseFull, $targetFull, $cmp)) {
        return '.'
    }

    # Se Target está dentro de Base: retorna o sufixo
    $prefix = $baseFull + $ds
    if ($targetFull.StartsWith($prefix, $cmp)) {
        return $targetFull.Substring($baseFull.Length + 1)
    }

    # Caso raro: fora da árvore (ex.: outra unidade). Devolve absoluto.
    return $targetFull
}

function Contar-ArquivosPorPasta {
    [OutputType([int64])]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Caminho,

        # Permitir coleção vazia para acumular as linhas
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [ValidateNotNull()]
        [System.Collections.IList]$Saida,

        # Raiz para calcular caminho relativo
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [string]$Raiz
    )

    try {
        $item = Get-Item -LiteralPath $Caminho -Force -ErrorAction Stop
    } catch {
        return 0
    }

    if (-not $item.PSIsContainer) { return 0 }

    # Ignora pastas cujo nome começa com "."
    if ($item.Name -and $item.Name.StartsWith('.')) { return 0 }

    $arquivosAqui = (
        Get-ChildItem -LiteralPath $item.FullName -File -Force -ErrorAction SilentlyContinue |
        Measure-Object
    ).Count

    [int64]$total = [int64]$arquivosAqui

    $subdirs = Get-ChildItem -LiteralPath $item.FullName -Directory -Force -ErrorAction SilentlyContinue |
               Where-Object { -not $_.Name.StartsWith('.') }

    foreach ($dir in $subdirs) {
        $total += Contar-ArquivosPorPasta -Caminho $dir.FullName -Saida $Saida -Raiz $Raiz
    }

    $rel = Get-RelativePath -BasePath $Raiz -TargetPath $item.FullName

    [void]$Saida.Add([pscustomobject]@{
        Pasta        = $rel
        ArquivosAqui = $arquivosAqui
        Total        = $total
    })

    return $total
}

# Resolve e normaliza a raiz
try {
    $raiz = (Resolve-Path -LiteralPath $Diretorio -ErrorAction Stop).Path
} catch {
    Write-Error "Diretorio inválido: '$Diretorio'. Detalhes: $($_.Exception.Message)"
    exit 1
}

$ds   = [IO.Path]::DirectorySeparatorChar
$ads  = [IO.Path]::AltDirectorySeparatorChar
$raiz = [IO.Path]::GetFullPath($raiz).TrimEnd($ds, $ads)

# Executa a contagem, acumulando linhas em memória
$linhas = [System.Collections.Generic.List[psobject]]::new()
$null = Contar-ArquivosPorPasta -Caminho $raiz -Saida $linhas -Raiz $raiz

# === Saída: listar na tela (console) ===

$linhas | Sort-Object Pasta | Format-Table -AutoSize

# Alternativas:
# $linhas | Sort-Object Total -Descending | Format-Table -AutoSize
# $linhas | Sort-Object Pasta | Format-Table -AutoSize
# $linhas | Sort-Object Total -Descending | Format-Table -AutoSize | Out-String -Width 200 | Out-Host