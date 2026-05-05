<template>
    <v-main class="bg-background">
        <!-- Hero Banner -->
        <v-sheet color="primary" class="hero-banner text-white text-center py-16 px-4" elevation="0">
            <v-container maxWidth="900" class="py-10">
                <h1 class="text-display-large font-weight-bold mb-6">Ocean Acidification & Hypoxia</h1>
                <!-- <p class="text-h6 font-weight-regular opacity-90 mb-0">
                    The Coastal Hypoxia, Ocean acidification, and 'Klimate' variables Evaluator.
                </p> -->
            </v-container>
        </v-sheet>

        <v-container class="mt-n10 content-container pb-12">
            <v-row>
                <!-- Overview Card -->
                <v-col cols="12">
                    <v-card elevation="3" class="rounded-xl pa-2 mb-2">
                        <v-card-text class="text-body-1 text-medium-emphasis pa-6" style="line-height: 1.8;">
                            The coastal waters of British Columbia are becoming warmer, more acidic, and less oxygenated as global and provincial emissions continue to rise. These shifts in the marine environment directly affect coastal Canadian communities and industry. 
                            <br><br>
                            The Ocean Acidification & Hypoxia app combines data from moored instruments and regional ocean models to provide users with live maps and climatological records of ocean temperature, salinity, pH, oxygen content, and the 'corrosiveness' of seawater to shelled organisms across southern to central BC. 
                            <br><br>
                            To learn more, visit <a href="https://www.oceanacidification.ca/" target="_blank" rel="noopener noreferrer" class="text-primary font-weight-bold text-decoration-none">Canada's Ocean Acidification Community of Practice</a> to discover our species impacts, action plans, Canada's expert database and more.
                        </v-card-text>
                    </v-card>
                </v-col>

                <!-- Two Column Layout: Who We Are & User Guide -->
                <v-col cols="12" md="6">
                    <v-card elevation="2" class="rounded-xl h-100 pa-2">
                        <v-card-title class="text-h5 font-weight-bold d-flex align-center pb-4 pl-4 pt-4">
                            <!-- <v-icon color="success" size="32" class="mr-4">mdi-account-multiple</v-icon> -->
                            Who We Are
                        </v-card-title>
                        <v-card-text class="text-body-1 text-medium-emphasis pl-4 pr-4" style="line-height: 1.6;">
                            The Ocean Acidification & Hypoxia app is developed and maintained by the Canadian Integrated Ocean Observing System (<a href="https://www.cioospacific.ca/" target="_blank" rel="noopener noreferrer" class="text-primary text-decoration-none font-weight-bold">CIOOS</a>) Pacific region in partnership with data providers, which include Ocean Networks Canada, Hakai Institute, Fisheries and Oceans Canada, the University of British Columbia, and the University of Washington.
                        </v-card-text>
                    </v-card>
                </v-col>

                <v-col cols="12" md="6">
                    <v-card elevation="2" class="rounded-xl h-100 pa-2">
                        <v-card-title class="text-h5 font-weight-bold d-flex align-center pb-4 pl-4 pt-4">
                            <!-- <v-icon color="warning" size="32" class="mr-4">mdi-book-open-page-variant</v-icon> -->
                            User Guide
                        </v-card-title>
                        <v-card-text class="text-body-1 text-medium-emphasis d-flex flex-column align-center justify-center pt-8">
                            <v-icon color="disabled" size="48" class="mb-4">mdi-hammer-wrench</v-icon>
                            <span class="text-h6 font-weight-light">Under construction - coming soon!</span>
                        </v-card-text>
                    </v-card>
                </v-col>

                <!-- Data Sources Card -->
                <v-col cols="12" class="mt-4">
                    <v-card elevation="2" class="rounded-xl pa-6">
                        <v-card-title class="text-h4 font-weight-bold d-flex align-center pb-6">
                            <!-- <v-icon color="info" size="36" class="mr-4">mdi-database-search</v-icon> -->
                            Data Sources
                        </v-card-title>
                        <v-card-text class="pt-0">
                            <!-- External Data Sources -->
                            <v-row class="mb-8">
                                <v-col cols="12">
                                    <div class="d-flex flex-column gap-6">
                                        <about-ssc />
                                        <about-liveocean />
                                        <about-nonna />
                                    </div>
                                </v-col>
                            </v-row>

                            <!-- Sensor Network -->
                            <v-divider class="mb-8"></v-divider>
                            <h3 class="text-h5 font-weight-bold d-flex align-center mb-6 text-primary">
                                <v-icon class="mr-3">mdi-access-point-network</v-icon>
                                Sensor Network
                            </h3>
                            
                            <div class="border rounded-lg overflow-hidden">
                                <v-table hover>
                                    <thead class="bg-surface-light">
                                        <tr>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Sensor details</th>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Point of Contact</th>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Location & Depth</th>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Variables</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr v-for="sensor in sensors" :key="sensor.id">
                                            <td class="py-3">
                                                <div class="font-weight-bold text-subtitle-1">{{ sensor.id }}</div>
                                                <div v-if="sensor.note" class="text-caption text-medium-emphasis mt-1">{{ sensor.note }}</div>
                                                <v-chip v-if="sensor.decommissioned" size="small" color="error" variant="flat" class="mt-2 text-caption font-weight-bold">
                                                    <v-icon start size="14">mdi-alert-circle</v-icon>
                                                    Decommissioned
                                                </v-chip>
                                            </td>
                                            <td class="py-3">
                                                <div class="font-weight-medium">{{ sensor.organization }}</div>
                                                <div class="text-body-2">{{ sensor.contactName }}</div>
                                                <a :href="`mailto:${sensor.contactEmail}`" class="text-body-2 text-decoration-none text-primary">{{ sensor.contactEmail }}</a>
                                            </td>
                                            <td class="py-3">
                                                <div class="d-flex align-center mb-1">
                                                    <v-icon size="16" class="mr-2" color="medium-emphasis">mdi-map-marker</v-icon>
                                                    <span class="text-body-2">{{ sensor.coordinates }}</span>
                                                </div>
                                                <div class="d-flex align-center">
                                                    <v-icon size="16" class="mr-2" color="medium-emphasis">mdi-arrow-down-box</v-icon>
                                                    <span class="text-body-2">{{ sensor.depth }}</span>
                                                </div>
                                            </td>
                                            <td class="py-3">
                                                <div class="text-body-2" style="max-width: 300px; line-height: 1.4;">{{ sensor.variables }}</div>
                                            </td>
                                        </tr>
                                    </tbody>
                                </v-table>
                            </div>
                        </v-card-text>
                    </v-card>
                </v-col>

                <!-- Sponsors Array -->
                <v-col cols="12" class="mt-12 mb-6">
                    <div class="text-center">
                        <h2 class="text-h5 font-weight-medium text-medium-emphasis mb-8">Supported By</h2>
                        <v-row justify="center" align="center" class="mx-auto" style="max-width: 600px;">
                            <v-col cols="12" sm="6" class="d-flex justify-center">
                                <!-- <v-card elevation="0" class="d-flex flex-column align-center justify-center rounded-lg h-100 w-100 pa-4" style="background-color: transparent;"> -->
                                    <v-img src="/logos/MEOPAR.webp" alt="MEOPAR Logo" max-height="80" contain></v-img>
                                <!-- </v-card> -->
                            </v-col>
                            <v-col cols="12" sm="6" class="d-flex justify-center">
                                <!-- <v-card elevation="0" class="d-flex flex-column align-center justify-center rounded-lg h-100 w-100 pa-4" style="background-color: transparent;"> -->
                                    <v-img src="/logos/DFO.png" alt="DFO Logo" max-height="80" contain></v-img>
                                <!-- </v-card> -->
                            </v-col>
                        </v-row>
                    </div>
                </v-col>
                
                <!-- Contact Us -->
                <v-col cols="12">
                    <v-card elevation="3" class="rounded-xl pa-2 bg-surface contact-box">
                        <v-card-text class="d-flex flex-column flex-md-row align-center justify-space-between py-8 px-6">
                            <div class="mb-6 mb-md-0 text-center text-md-left">
                                <h3 class="text-h4 font-weight-bold mb-3 d-flex align-center justify-center justify-md-start">
                                    <!-- <v-icon color="secondary" size="36" class="mr-3">mdi-email-fast</v-icon> -->
                                    Get in Touch
                                </h3>
                                <p class="text-body-1 text-medium-emphasis mb-0" style="max-width: 600px;">
                                    Have feedback or datasets you'd like to see incorporated into the Ocean Acidification & Hypoxia app? We'd love to hear from you.
                                </p>
                            </div>
                            <div class="d-flex flex-column flex-sm-row gap-4">
                                <v-btn 
                                    prepend-icon="mdi-email" 
                                    color="secondary" 
                                    variant="flat" 
                                    size="large"
                                    class="text-none font-weight-bold rounded-lg mx-2 my-2 my-sm-0" 
                                    href="mailto:yayla.sezginer@cioospacific.ca"
                                >
                                    Yayla Sezginer
                                </v-btn>
                                <v-btn 
                                    prepend-icon="mdi-email" 
                                    color="secondary" 
                                    variant="flat" 
                                    size="large"
                                    class="text-none font-weight-bold rounded-lg mx-2 my-2 my-sm-0" 
                                    href="mailto:taimazb@oceannetworks.ca"
                                >
                                    Taimaz Bahadory
                                </v-btn>
                            </div>
                        </v-card-text>
                    </v-card>
                </v-col>

            </v-row>
        </v-container>
    </v-main>
</template>

<script setup lang="ts">
interface Sensor {
    id: string
    note?: string
    organization: string
    contactName: string
    contactEmail: string
    coordinates: string
    variables: string
    depth: string
    orgClass: 'org-hakai' | 'org-onc'
    decommissioned?: boolean
}

const sensors: Sensor[] = [
    {
        id: 'Kwakshua Channel CO2 sensor',
        organization: 'Hakai Institute',
        contactName: 'Wiley Evans',
        contactEmail: 'wiley.evans@hakai.org',
        coordinates: '51.65, -127.97',
        variables: 'pCO2 (uatm), Temp (degC), Salinity (g/kg)',
        depth: 'Surface',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bamfield Marine Science Centre Burke-o-Lator',
        organization: 'Hakai Institute',
        contactName: 'Wiley Evans',
        contactEmail: 'wiley.evans@hakai.org',
        coordinates: '48.837, -125.136',
        variables: 'pCO2 (uatm), DIC (umol/kg), Temp (degC), Salinity (g/kg), Total Alkalinity (umol/kg), pH, Omega aragonite, Omega calcite',
        depth: '20m',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bute Inlet',
    },
    {
        id: 'Quadra Island Hyacinthe Bay Burke-o-Lator',
        organization: 'Hakai Institute',
        contactName: 'Wiley Evans',
        contactEmail: 'wiley.evans@hakai.org',
        coordinates: '50.116, -125.222',
        variables: 'Temp (degC), Salinity (g/kg), pCO2 (uatm), pH, Total Alkalinity (umol/kg), DIC (umol/kg), Omega aragonite, Omega calcite',
        depth: 'Surface',
        orgClass: 'org-hakai'
    },
    {
        id: 'Folger Pinnacle',
        organization: 'Ocean Networks Canada',
        contactName: 'Stef Mellon',
        contactEmail: 'smellon@uvic.ca',
        coordinates: '48.81, -125.28',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '25m',
        orgClass: 'org-onc'
    },
    {
        id: 'Folger Deep',
        organization: 'Ocean Networks Canada',
        contactName: 'Stef Mellon',
        contactEmail: 'smellon@uvic.ca',
        coordinates: '48.8082916667, -125.2815',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '95m',
        orgClass: 'org-onc'
    },
    {
        id: 'Baynes Sound Profiling Instrument',
        organization: 'Ocean Networks Canada',
        contactName: 'Zarah Zheng',
        contactEmail: 'zarahzheng@uvic.ca',
        coordinates: '49.487, -124.7693',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: 'Full water column',
        orgClass: 'org-onc'
    },
    {
        id: 'Baynes Sound Historical Mooring',
        organization: 'Ocean Networks Canada',
        contactName: 'Zarah Zheng',
        contactEmail: 'zarahzheng@uvic.ca',
        coordinates: '49.487, -124.7693',
        variables: 'pCO2 (uatm), O2 (mL/L), Temp',
        depth: '5, 20, 40m',
        orgClass: 'org-onc',
        decommissioned: true
    },
    {
        id: 'Central Strait of Georgia platform',
        organization: 'Ocean Networks Canada',
        contactName: 'Alice Bui',
        contactEmail: 'aovbui@uvic.ca',
        coordinates: '49.05, -123.42',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '300m',
        orgClass: 'org-onc'
    },
    {
        id: 'Strait of Georgia East',
        organization: 'Ocean Networks Canada',
        contactName: 'Alice Bui',
        contactEmail: 'aovbui@uvic.ca',
        coordinates: '49.04, -123.32',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '170m',
        orgClass: 'org-onc'
    }
]
</script>

<style scoped>
.hero-banner {
    background: linear-gradient(rgba(30, 58, 138, 0.7), rgba(15, 23, 42, 0.8)), url('https://images.unsplash.com/photo-1582967788606-a171c1080cb0?q=80&w=2070&auto=format&fit=crop') no-repeat center center;
    background-size: cover;
    position: relative;
    overflow: hidden;
}
.hero-banner::after {
    content: '';
    position: absolute;
    bottom: 0;
    left: 0;
    width: 100%;
    height: 30%;
    background: url('data:image/svg+xml;utf8,<svg viewBox="0 0 100 20" xmlns="http://www.w3.org/2000/svg"><path d="M0 20 Q 25 0 50 20 T 100 20 L 100 20 L 0 20 Z" fill="rgba(255,255,255,0.05)"/></svg>') no-repeat bottom;
    background-size: cover;
    pointer-events: none;
}
.content-container {
    position: relative;
    z-index: 1;
}
.contact-box {
    border-top: 4px solid rgb(var(--v-theme-secondary));
}
.gap-4 {
    gap: 16px;
}
</style>
