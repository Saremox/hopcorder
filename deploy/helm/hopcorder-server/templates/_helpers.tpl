{{- define "hopcorder-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hopcorder-server.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "hopcorder-server.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "hopcorder-server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "hopcorder-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hopcorder-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* The configuration file: the user's config plus what the chart owns. */}}
{{- define "hopcorder-server.config" -}}
{{- if (default dict .Values.config.clickhouse).password }}
{{- fail "config.clickhouse.password: put the password in a Secret and set clickhouse.passwordSecret.name (in config it would land in a ConfigMap)" }}
{{- end }}
{{- $cfg := deepCopy .Values.config }}
{{- $_ := set $cfg "listen" (printf "0.0.0.0:%d" (int .Values.service.port)) }}
{{- $ch := default dict $cfg.clickhouse }}
{{- if .Values.clickhouse.passwordSecret.name }}
{{- $_ := set $ch "password_file" "/etc/hopcorder/secrets/clickhouse-password" }}
{{- end }}
{{- $_ := set $cfg "clickhouse" $ch }}
{{- $enrich := default dict $cfg.enrich }}
{{- $asn := default dict $enrich.asn }}
{{- $_ := set $asn "file" "/var/lib/hopcorder-server/ip2asn-combined.tsv.gz" }}
{{- $_ := set $enrich "asn" $asn }}
{{- $_ := set $cfg "enrich" $enrich }}
{{- toYaml $cfg }}
{{- end }}
