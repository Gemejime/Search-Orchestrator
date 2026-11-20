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
      ports:
        - name: http
          containerPort: {{ .Values.service.httpPort }}
          protocol: TCP
        - name: grpc
          containerPort: {{ .Values.service.grpcPort }}
          protocol: TCP
      env:
        - name: GOMEMLIMIT
          value: {{ .Values.env.GOMEMLIMIT | quote }}
        - name: GOGC
          value: {{ .Values.env.GOGC | quote }}
        - name: HTTP_ADDR
          value: {{ .Values.env.HTTP_ADDR | quote }}
        - name: GRPC_ADDR
          value: {{ .Values.env.GRPC_ADDR | quote }}
        - name: REDIS_ADDRS
          value: {{ .Values.env.REDIS_ADDRS | quote }}
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: {{ include "rag-search-orchestrator.redisSecretName" . }}
              key: {{ .Values.redis.secretKey | quote }}
        - name: HNSW_TARGETS
          value: {{ .Values.env.HNSW_TARGETS | quote }}
        - name: BM25_TARGETS
          value: {{ .Values.env.BM25_TARGETS | quote }}
        - name: TRITON_EMBED_TARGETS
          value: {{ .Values.env.TRITON_EMBED_TARGETS | quote }}
        - name: TRITON_RERANK_TARGETS
          value: {{ .Values.env.TRITON_RERANK_TARGETS | quote }}
        - name: TRITON_EMBED_MODEL
          value: {{ .Values.env.TRITON_EMBED_MODEL | quote }}
        - name: TRITON_RERANK_MODEL
          value: {{ .Values.env.TRITON_RERANK_MODEL | quote }}
        - name: TRITON_RERANK_VERSION
          value: {{ .Values.env.TRITON_RERANK_VERSION | quote }}
        - name: RERANK_TAU
          value: {{ .Values.env.RERANK_TAU | quote }}
        - name: CACHE_TTL
          value: {{ .Values.env.CACHE_TTL | quote }}
        - name: RING_BUFFER_CAPACITY
          value: {{ .Values.env.RING_BUFFER_CAPACITY | quote }}
        - name: GRPC_POOL_SIZE
          value: {{ .Values.env.GRPC_POOL_SIZE | quote }}
        - name: SHUTDOWN_DRAIN_DELAY
          value: {{ .Values.env.SHUTDOWN_DRAIN_DELAY | quote }}
        - name: LOG_LEVEL
          value: {{ .Values.env.LOG_LEVEL | quote }}
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
