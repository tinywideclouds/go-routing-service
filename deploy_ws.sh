gcloud run deploy routing-service-ws \
  --source . \
  --project gemini-power-test \
  --region europe-west1 \
  --allow-unauthenticated \
  --args=-ws=true \
  --set-env-vars="GCP_PROJECT_ID=gemini-power-test" \
  --set-env-vars="CORS_ALLOWED_ORIGINS=https://messenger-app-885150127230.europe-west1.run.app" \
  --set-env-vars="IDENTITY_SERVICE_URL=https://node-identity-service-885150127230.europe-west1.run.app" \