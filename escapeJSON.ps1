
function Escape-JsonString {
    param([string]$s)
    if ($null -eq $s) { return "" }
    $sb = [System.Text.StringBuilder]::new()
    foreach ($ch in $s.ToCharArray()) {
        switch ($ch) {
            "`b" { $null = $sb.Append('\b') }
            "`t" { $null = $sb.Append('\t') }
            "`n" { $null = $sb.Append('\n') }
            "`f" { $null = $sb.Append('\f') }
            "`r" { $null = $sb.Append('\r') }
            '"'  { $null = $sb.Append('\"') }
            '\'  { $null = $sb.Append('\\') }
            '/'  { $null = $sb.Append('\/') }
            default {
                $code = [int][char]$ch
                if ($code -lt 0x20) {
                    $null = $sb.Append('\u' + $code.ToString('x4'))
                } else {
                    $null = $sb.Append($ch)
                }
            }
        }
    }
    $sb.ToString()
}
