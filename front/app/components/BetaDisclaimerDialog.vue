<template>
    <v-dialog v-model="showDialog" width="600" persistent @update:model-value="handleDialogToggle">
        <v-card>
            <v-card-title class="beta-header pa-6">
                <div class="text-h5 font-weight-bold text-white">Beta Version Notice</div>
                <div class="text-caption text-white" style="opacity: 0.9;">Help us improve</div>
            </v-card-title>

            <v-card-text class="pa-6">
                <div class="mb-4">
                    <p class="text-h6  mb-2">Welcome to CHOKE</p>
                    <p>
                        This application is currently in <strong>beta</strong> and actively under development.
                        Features, data, and functionality may change as we continue to improve the platform.
                    </p>
                </div>

                <div class="mb-4">
                    <p class="text-h6  mb-2">We Value Your Feedback</p>
                    <p>
                        Your input is crucial in helping us build a better tool. Whether you encounter issues,
                        have suggestions, or want to share your experience, we'd love to hear from you.
                    </p>
                </div>

                <div class="mb-6">
                    <div class="d-flex gap-2 flex-wrap">
                        <v-btn color="primary" variant="outlined" size="small" prepend-icon="mdi-open-in-new" class="mx-2"
                            :to="{ name: 'about' }" @click="showDialog = false">
                            Learn More
                        </v-btn>
                        <v-btn color="info" variant="outlined" size="small" prepend-icon="mdi-form-textarea" class="mx-2"
                            href="https://docs.google.com/forms/d/e/1FAIpQLSdtkCjo2RVH0uVbHo7sWuRZHrvenxM2hMsZgW9Ou61WDYBFxg/viewform?usp=dialog"
                            target="_blank" rel="noopener noreferrer">
                            Feedback Survey
                        </v-btn>
                    </div>
                </div>

                <v-divider class="my-4"></v-divider>

                <div class="d-flex align-center gap-2">
                    <v-checkbox v-model="dontShowAgain" label="Don't show this again" hide-details
                        density="compact"></v-checkbox>
                </div>
            </v-card-text>

            <v-card-actions class="pa-4">
                <v-spacer></v-spacer>
                <v-btn color="error" variant="tonal" @click="closeDialog">
                    Close
                </v-btn>
            </v-card-actions>
        </v-card>
    </v-dialog>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const showDialog = ref(true)
const dontShowAgain = ref(false)

const STORAGE_KEY = 'choke_beta_disclaimer_dismissed'

onMounted(() => {
    // Check if user has dismissed this before
    const isDismissed = localStorage.getItem(STORAGE_KEY) === 'true'
    if (isDismissed) {
        showDialog.value = false
    }
})

const closeDialog = () => {
    if (dontShowAgain.value) {
        localStorage.setItem(STORAGE_KEY, 'true')
    }
    showDialog.value = false
}

const handleDialogToggle = (value: boolean) => {
    if (!value) {
        // Dialog is closing
        if (dontShowAgain.value) {
            localStorage.setItem(STORAGE_KEY, 'true')
        }
    }
}
</script>

<style scoped>
.bg-opacity-10 {
    background-color: rgba(var(--v-theme-warning-rgb), 0.1);
}

.beta-header {
    background: linear-gradient(135deg, #0098ff 0%, #fb8c00 100%);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.25);
    border-radius: 4px 4px 0 0;
}
</style>
