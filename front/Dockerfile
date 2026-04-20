# Use official Node.js LTS image
FROM node:22-alpine AS base

# Set working directory
WORKDIR /app

# Copy package files
COPY package.json package-lock.json* pnpm-lock.yaml* yarn.lock* ./

# Development stage
FROM base AS development

# Install all dependencies (including dev dependencies)
RUN npm install --legacy-peer-deps

# Copy all source files
COPY . .

# Expose default Nuxt port
EXPOSE 3000

# Start Nuxt in dev mode
CMD ["npm", "run", "dev"]


# Production stage
FROM base AS production

ARG NUXT_PUBLIC_API_BASE_URL
ENV NUXT_PUBLIC_API_BASE_URL=${NUXT_PUBLIC_API_BASE_URL}

# Install all dependencies (needed for build)
RUN npm install --legacy-peer-deps

# Copy all source files
COPY . .

# Build the Nuxt app
RUN npm run build

# Remove dev dependencies to reduce final image size
# RUN npm prune --omit=dev

# Expose default Nuxt port
EXPOSE 3000

# Start Nuxt in production mode
CMD ["npm", "run", "start"]