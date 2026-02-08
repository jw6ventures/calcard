{{- define "calcard.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "calcard.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "calcard.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "calcard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "calcard.selectorLabels" -}}
app.kubernetes.io/name: {{ include "calcard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "calcard.postgresHost" -}}
{{- if .Values.app.db.host -}}
{{- .Values.app.db.host -}}
{{- else -}}
{{- printf "%s-postgres" (include "calcard.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "calcard.ingressHost" -}}
{{- if .Values.ingress.host -}}
{{- .Values.ingress.host -}}
{{- else -}}
{{- $url := required "app.baseUrl is required to derive ingress.host" .Values.app.baseUrl -}}
{{- regexReplaceAll "^(https?://)?([^/]+).*$" $url "$2" -}}
{{- end -}}
{{- end -}}
