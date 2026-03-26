<template>
  <v-app theme="dark">
    <v-app-bar density="compact" class="px-5" app>
      <NuxtLink to="/" class="logo-link">
        <img src="/OA_logo.png" alt="OA Logo" class="logo-icon" />
      </NuxtLink>
      <v-app-bar-title>OA</v-app-bar-title>
      <v-spacer></v-spacer>
      <!-- Optional: Add menu items here -->
      <NuxtLink to="/modeleval">
        <v-btn density="compact" text>Model Evaluation</v-btn>
      </NuxtLink>
      <NuxtLink to="/about">
        <v-btn density="compact" text>About</v-btn>
      </NuxtLink>
    </v-app-bar>

    <NuxtRouteAnnouncer />
    <NuxtPage />
  </v-app>
</template>

<style scoped>
.logo-link {
  display: flex;
  align-items: center;
  text-decoration: none;
  cursor: pointer;
}

.logo-icon {
  height: 40px;
  width: 40px;
  margin-right: 16px;
  border-radius: 4px;
}

.logo-icon:hover {
  opacity: 0.8;
  transition: opacity 0.2s;
}
</style>