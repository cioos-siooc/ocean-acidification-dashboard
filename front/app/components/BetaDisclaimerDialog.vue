<template>
    <UModal v-model:open="showDialog" :dismissible="false" :ui="{ content: 'max-w-[600px]' }"
        @update:open="handleDialogToggle">
        <template #content>
        <div class="bg-elevated rounded-lg">
            <div class="text-lg beta-header p-6">
                <div class="text-2xl font-bold text-white">Beta Version Notice</div>
                <div class="text-xs text-white" style="opacity: 0.9;">Help us improve</div>
            </div>

            <div class="p-6">
                <div class="mb-4">
                    <p class="text-[22px] mb-2">Welcome to the OceanECO app</p>
                    <p>
                        This application is currently in <strong>beta</strong> and actively under development.
                        Features, data, and functionality may change as we continue to improve the platform.
                    </p>
                </div>

                <div class="mb-4">
                    <p class="text-[22px] mb-2">We Value Your Feedback</p>
                    <p>
                        Your input is crucial in helping us build a better tool. Whether you encounter issues,
                        have suggestions, or want to share your experience, we'd love to hear from you.
                    </p>
                </div>

                <div class="mb-6">
                    <div class="flex gap-2 flex-wrap">
                        <UButton variant="outline" color="primary" class="mx-2 p-3" :to="{ name: 'about' }" @click="showDialog = false">
                            Learn More
                        </UButton>
                        <UButton variant="outline" color="info" href="https://docs.google.com/forms/d/e/1FAIpQLSdGiIclM5wvIbPReZydsXKiRBXbZsQVEdoQPlA0EruKIoNJkg/viewform?usp=dialog" class="mx-2 p-3" target="_blank" rel="noopener noreferrer">
                            Feedback Survey
                        </UButton>
                    </div>
                </div>

                <USeparator class="my-4" />

                <div class="flex items-center gap-2">
                    <UCheckbox v-model="dontShowAgain" label="Don't show this again" />
                </div>
            </div>

            <div class="flex items-center gap-2 p-4">
                <div class="grow" />
                <UButton variant="subtle" color="error" class="p-3" @click="closeDialog">
                    Close
                </UButton>
            </div>
        </div>
        </template>
    </UModal>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const showDialog = ref(true)
const dontShowAgain = ref(false)

const STORAGE_KEY = 'oah_beta_disclaimer_dismissed'

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
