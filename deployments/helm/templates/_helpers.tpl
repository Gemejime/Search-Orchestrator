{{- define "rag-search-orchestrator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rag-search-orchestrator.fullname" -}}
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

{{- define "rag-search-orchestrator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "rag-search-orchestrator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "rag-search-orchestrator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-search-orchestrator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "rag-search-orchestrator.redisSecretName" -}}
{{- if .Values.redis.existingSecret }}
{{- .Values.redis.existingSecret }}
{{- else }}
{{- printf "%s-redis" (include "rag-search-orchestrator.fullname" .) }}
{{- end }}
{{- end }}

{{- define "rag-search-orchestrator.podTemplateSpec" -}}
metadata:
  annotations:
    checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
    {{- if not .Values.redis.existingSecret }}
    checksum/secret: {{ include (print $.Template.BasePath "/secret.yaml") . | sha256sum }}
    {{- else }}
    secret.reloader.stakater.com/reload: {{ include "rag-search-orchestrator.redisSecretName" . | quote }}
    {{- end }}
  labels:
    {{- include "rag-search-orchestrator.selectorLabels" . | nindent 4 }}
spec:
  terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    fsGroup: 65532
    seccompProfile:
      type: RuntimeDefault
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: topology.kubernetes.io/zone
      whenUnsatisfiable: ScheduleAnyway
      labelSelector:
        matchLabels:
          {{- include "rag-search-orchestrator.selectorLabels" . | nindent 10 }}
  containers:
    - name: {{ include "rag-search-orchestrator.name" . }}
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        readOnlyRootFilesystem: true
        allowPrivilegeEscalation: false
        capabilities:
          drop:
            - ALL
      {{- $httpPort := regexFind "[0-9]+$" (default ":8080" .Values.env.HTTP_ADDR) | default "8080" | int }}
      {{- $grpcPort := regexFind "[0-9]+$" (default ":50051" .Values.env.GRPC_ADDR) | default "50051" | int }}
      ports:
        - name: http
          containerPort: {{ $httpPort }}
          protocol: TCP
        - name: grpc
          containerPort: {{ $grpcPort }}
          protocol: TCP
      envFrom:
        - configMapRef:
            name: {{ include "rag-search-orchestrator.fullname" . }}-config
      env:
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: {{ include "rag-search-orchestrator.redisSecretName" . }}
              key: {{ .Values.redis.secretKey | quote }}
      livenessProbe:
        httpGet:
          path: /healthz
          port: http
        initialDelaySeconds: 5
        periodSeconds: 10
        timeoutSeconds: 2
      readinessProbe:
        httpGet:
          path: /ready
          port: http
        initialDelaySeconds: 5
        periodSeconds: 5
        timeoutSeconds: 2
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
{{- end }}
