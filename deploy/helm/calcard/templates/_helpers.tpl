{{- /*
calcard.integer renders a value as plain decimal integer text, taking
(list "values.path" value minimum) or (list "values.path" value minimum maximum).
A string of decimal digits is accepted for the keys whose schema allows one. Helm reads every number in a values file as
a float64, which prints a million or more in exponent form (67108864 becomes
6.7108864e+07) that neither the config loader nor a Kubernetes integer field
accepts. values.schema.json types these keys as integers; this refuses the same
values again for installs run with --skip-schema-validation, where int64 alone
would truncate 1.5 to 1 and turn "abc" into 0.
*/ -}}
{{- define "calcard.integer" -}}
{{- $path := index . 0 -}}
{{- $value := index . 1 -}}
{{- $minimum := index . 2 -}}
{{- $bounded := gt (len .) 3 -}}
{{- $text := "" -}}
{{- if kindIs "float64" $value -}}
{{- if eq $value (float64 (int64 $value)) -}}
{{- $text = toString (int64 $value) -}}
{{- end -}}
{{- else if or (kindIs "int" $value) (kindIs "int64" $value) -}}
{{- $text = toString $value -}}
{{- else if kindIs "string" $value -}}
{{- if eq $value (toString (int64 $value)) -}}
{{- $text = $value -}}
{{- end -}}
{{- end -}}
{{- $valid := and (ne $text "") (ge (int64 $text) (int64 $minimum)) -}}
{{- if and $valid $bounded -}}
{{- $valid = le (int64 $text) (int64 (index . 3)) -}}
{{- end -}}
{{- if and (not $valid) $bounded -}}
{{- fail (printf "%s must be an integer from %d to %d (got %v)" $path $minimum (index . 3) $value) -}}
{{- else if not $valid -}}
{{- fail (printf "%s must be an integer of at least %d (got %v)" $path $minimum $value) -}}
{{- end -}}
{{- $text -}}
{{- end -}}
