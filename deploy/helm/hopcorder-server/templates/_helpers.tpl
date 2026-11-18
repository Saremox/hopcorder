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

{{/* The bundled ClickHouse (clickhouse.deploy): its name and labels. */}}
{{- define "hopcorder-server.clickhouseName" -}}
{{- printf "%s-clickhouse" (include "hopcorder-server.fullname" . | trunc 52 | trimSuffix "-") }}
{{- end }}

{{- define "hopcorder-server.clickhouseSelectorLabels" -}}
app.kubernetes.io/name: clickhouse
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: hopcorder-clickhouse
{{- end }}

{{/* The Secret holding the server's ClickHouse password, if there is one. */}}
{{- define "hopcorder-server.clickhousePasswordSecret" -}}
{{- if .Values.clickhouse.passwordSecret.name }}
{{- .Values.clickhouse.passwordSecret.name }}
{{- else if .Values.clickhouse.deploy.enabled }}
{{- include "hopcorder-server.clickhouseName" . }}
{{- end }}
{{- end }}

{{/* The key of the server's ClickHouse password in that Secret. */}}
{{- define "hopcorder-server.clickhousePasswordKey" -}}
{{- if .Values.clickhouse.passwordSecret.name }}{{ .Values.clickhouse.passwordSecret.key }}{{ else }}password{{ end }}
{{- end }}

{{/* The Secret holding the bundled ClickHouse's read-only user's password. */}}
{{- define "hopcorder-server.clickhouseReadOnlySecret" -}}
{{- .Values.clickhouse.deploy.readOnlyUser.passwordSecret.name | default (printf "%s-readonly" (include "hopcorder-server.clickhouseName" .)) }}
{{- end }}

{{- define "hopcorder-server.clickhouseReadOnlyKey" -}}
{{- if .Values.clickhouse.deploy.readOnlyUser.passwordSecret.name }}{{ .Values.clickhouse.deploy.readOnlyUser.passwordSecret.key }}{{ else }}password{{ end }}
{{- end }}

{{/*
A generated password: the existing Secret's value if there is one (so an
upgrade keeps it), else a new random one. Takes (list $ secretName).
lookup finds nothing without the cluster (helm template, Argo CD), so there
every render has a new one; the Secrets are immutable for that (see
clickhouse.yaml). No checksum of them on the pods: under those renderers it
would restart ClickHouse and the server on every sync.
*/}}
{{- define "hopcorder-server.generatedPassword" -}}
{{- $root := index . 0 }}
{{- $existing := lookup "v1" "Secret" $root.Release.Namespace (index . 1) }}
{{- if and $existing $existing.data $existing.data.password }}
{{- $existing.data.password }}
{{- else }}
{{- randAlphaNum 32 | b64enc }}
{{- end }}
{{- end }}

{{/*
The Host names the server answers, against DNS rebinding (allowedHosts in
values.yaml), as a YAML list: the localhost names (kubectl port-forward), the
Service's names, the ingress hosts and allowedHosts; or ["*"] (no check) if
allowedHosts has "*". With an ingress rule without host, or a NodePort or
LoadBalancer Service, the chart cannot know the names clients use, so
allowedHosts must list them.
*/}}
{{- define "hopcorder-server.allowedHosts" -}}
{{- $extra := .Values.allowedHosts | default list }}
{{- if has "*" $extra }}
{{- list "*" | toYaml }}
{{- else }}
{{- $full := include "hopcorder-server.fullname" . }}
{{- $ns := .Release.Namespace }}
{{- $hosts := list "localhost" "127.0.0.1" "::1" $full (printf "%s.%s" $full $ns) (printf "%s.%s.svc" $full $ns) (printf "%s.%s.svc.%s" $full $ns (.Values.clusterDomain | default "cluster.local")) }}
{{- if .Values.ingress.enabled }}
{{- range .Values.ingress.hosts }}
{{- if .host }}
{{- $hosts = append $hosts .host }}
{{- else if not $extra }}
{{- fail "ingress.hosts: a rule without host answers any name: list the names clients use in allowedHosts (the server answers only the names it knows, against DNS rebinding), or set allowedHosts to [\"*\"] to turn that check off" }}
{{- end }}
{{- end }}
{{- end }}
{{- if and (has .Values.service.type (list "NodePort" "LoadBalancer")) (not $extra) }}
{{- fail (printf "service.type %s: list the names and addresses clients use for it in allowedHosts (the server answers only the names it knows, against DNS rebinding), or set allowedHosts to [\"*\"] to turn that check off" .Values.service.type) }}
{{- end }}
{{- range $h := concat $hosts $extra }}
{{- if or (eq (toString $h) "*") (not (include "hopcorder-server.validHost" (toString $h))) }}
{{- fail (printf "allowedHosts: %q is not a host name, *.domain (a domain of at least two labels) or IP address without port" (toString $h)) }}
{{- end }}
{{- end }}
{{- concat $hosts $extra | uniq | toYaml }}
{{- end }}
{{- end }}

{{/*
"true" if the server takes . as an allowed_hosts entry (validAllowedHost in
internal/config/server.go), else empty: "*"; an IPv4 address (dotted quad);
an IPv6 address (RFC 3986's forms), bare or in brackets; a host name of at
most 253 characters (and a trailing dot) in labels of 1 to 63 letters,
digits, - and _, not starting or ending with -, the last not all digits; or
*. and such a name of at least two labels. No ports. It may refuse what the
server would take, never the other way round: the server refuses its
config at start, and the pod would crash-loop instead of the install
failing.
*/}}
{{- define "hopcorder-server.validHost" -}}
{{- $h := toString . }}
{{- $octet := "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])" }}
{{- $v4 := printf "%[1]s(\\.%[1]s){3}" $octet }}
{{- $h16 := "[0-9A-Fa-f]{1,4}" }}
{{- $ls32 := printf "(%[1]s:%[1]s|%[2]s)" $h16 $v4 }}
{{- $v6 := printf "^((%[1]s:){6}%[2]s|::(%[1]s:){5}%[2]s|(%[1]s)?::(%[1]s:){4}%[2]s|((%[1]s:){0,1}%[1]s)?::(%[1]s:){3}%[2]s|((%[1]s:){0,2}%[1]s)?::(%[1]s:){2}%[2]s|((%[1]s:){0,3}%[1]s)?::%[1]s:%[2]s|((%[1]s:){0,4}%[1]s)?::%[2]s|((%[1]s:){0,5}%[1]s)?::%[1]s|((%[1]s:){0,6}%[1]s)?::)$" $h16 $ls32 }}
{{- $label := "[A-Za-z0-9_]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?" }}
{{- $name := trimSuffix "." (trimPrefix "*." $h) }}
{{- $bare := trimSuffix "]" (trimPrefix "[" $h) }}
{{- if eq $h "*" }}true
{{- else if regexMatch (printf "^%s$" $v4) $h }}true
{{- else if and (or (eq $bare $h) (eq (printf "[%s]" $bare) $h)) (regexMatch $v6 $bare) }}true
{{- else if and (regexMatch (printf "^(%[1]s\\.)*%[1]s$" $label) $name) (le (len $name) 253) (not (regexMatch "(^|\\.)[0-9]+$" $name)) (or (not (hasPrefix "*." $h)) (contains "." $name)) }}true
{{- end }}
{{- end }}

{{/*
The bundled ClickHouse's memory limit (clickhouse.deploy.resources.limits.memory)
in MiB, 0 without one.
*/}}
{{- define "hopcorder-server.clickhouseMemoryMiB" -}}
{{- $m := toString (dig "limits" "memory" "" (.Values.clickhouse.deploy.resources | default dict)) }}
{{- $num := "^[0-9]+(\\.[0-9]+)?" }}
{{- if regexMatch (printf "%s(k|M|G|T|Ki|Mi|Gi|Ti)?$" $num) $m }}
{{- $unit := get (dict "" 1.0 "k" 1e3 "M" 1e6 "G" 1e9 "T" 1e12 "Ki" 1024.0 "Mi" 1048576.0 "Gi" 1073741824.0 "Ti" 1099511627776.0) (regexReplaceAll $num $m "") }}
{{- divf (mulf (regexFind $num $m | float64) $unit) 1048576 | floor | int64 }}
{{- else if $m }}
{{- fail (printf "clickhouse.deploy.resources.limits.memory %q: write it in bytes or with a k, M, G, T, Ki, Mi, Gi or Ti suffix (the chart sizes the read API's budget from it), or set config.query.max_memory_bytes" $m) }}
{{- else }}0
{{- end }}
{{- end }}

{{/* The configuration file: the user's config plus what the chart owns. */}}
{{- define "hopcorder-server.config" -}}
{{- if (default dict .Values.config.clickhouse).password }}
{{- fail "config.clickhouse.password: put the password in a Secret and set clickhouse.passwordSecret.name (in config it would land in a ConfigMap)" }}
{{- end }}
{{- $cfg := deepCopy .Values.config }}
{{- $_ := set $cfg "listen" (printf "0.0.0.0:%d" (int .Values.service.port)) }}
{{- if not $cfg.allowed_hosts }}
{{- $_ := set $cfg "allowed_hosts" (include "hopcorder-server.allowedHosts" . | fromYamlArray) }}
{{- else }}
{{- range $h := $cfg.allowed_hosts }}
{{- if not (include "hopcorder-server.validHost" (toString $h)) }}
{{- fail (printf "config.allowed_hosts: %q is not a host name, *.domain (a domain of at least two labels) or IP address without port, or \"*\"" (toString $h)) }}
{{- end }}
{{- end }}
{{- end }}
{{- $ch := default dict $cfg.clickhouse }}
{{- if .Values.clickhouse.deploy.enabled }}
{{- $_ := set $ch "url" (printf "http://%s:8123" (include "hopcorder-server.clickhouseName" .)) }}
{{- $_ := set $ch "user" .Values.clickhouse.deploy.user }}
{{- /*
Read budgets that fit the bundled ClickHouse (round four): read queries may
use at most a third of what it may use (80% of its memory limit), two at a
time, unless config.query says otherwise. The server's defaults (1GiB a
query, 8 at once) are for a large ClickHouse: 2Gi gives 273MiB and 2.
*/}}
{{- $q := default dict $cfg.query }}
{{- if not (hasKey $q "max_concurrent") }}
{{- $_ := set $q "max_concurrent" 2 }}
{{- end }}
{{- if not (hasKey $q "max_memory_bytes") }}
{{- $mib := include "hopcorder-server.clickhouseMemoryMiB" . | int64 }}
{{- if gt $mib 0 }}
{{- $_ := set $q "max_memory_bytes" (printf "%dMiB" (max 16 (div (mul $mib 8) (mul 30 (int64 $q.max_concurrent))))) }}
{{- end }}
{{- end }}
{{- $_ := set $cfg "query" $q }}
{{- end }}
{{- if include "hopcorder-server.clickhousePasswordSecret" . }}
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
