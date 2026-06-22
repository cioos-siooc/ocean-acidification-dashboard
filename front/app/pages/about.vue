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
                            The coastal waters of British Columbia are becoming warmer, more acidic, and less oxygenated
                            as global and provincial emissions continue to rise. These shifts in the marine environment
                            directly affect coastal Canadian communities and industry.
                            <br><br>
                            The Ocean Acidification & Hypoxia app combines data from moored instruments and regional
                            ocean models to provide users with live maps and climatological records of ocean
                            temperature, salinity, pH, oxygen content, and the 'corrosiveness' of seawater to shelled
                            organisms across southern to central BC.
                            <br><br>
                            To learn more, visit <a href="https://www.oceanacidification.ca/" target="_blank"
                                rel="noopener noreferrer"
                                class="text-primary font-weight-bold text-decoration-none">Canada's Ocean Acidification
                                Community of Practice</a> to discover our species impacts, action plans, Canada's expert
                            database and more.
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
                            The Ocean Acidification & Hypoxia app is developed and maintained by the Canadian Integrated
                            Ocean Observing System (<a href="https://www.cioospacific.ca/" target="_blank"
                                rel="noopener noreferrer"
                                class="text-primary text-decoration-none font-weight-bold">CIOOS</a>) Pacific region in
                            partnership with data providers, which include Ocean Networks Canada, Hakai Institute,
                            Fisheries and Oceans Canada, the University of British Columbia, and the University of
                            Washington.
                        </v-card-text>
                    </v-card>
                </v-col>

                <v-col cols="12" md="6">
                    <v-card elevation="2" class="rounded-xl h-100 pa-2">
                        <v-card-title class="text-h5 font-weight-bold d-flex align-center pb-4 pl-4 pt-4">
                            <!-- <v-icon color="warning" size="32" class="mr-4">mdi-book-open-page-variant</v-icon> -->
                            User Guide
                        </v-card-title>
                        <v-card-text
                            class="text-body-1 text-medium-emphasis d-flex flex-column align-center justify-center pt-8">
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
                                            <th class="text-subtitle-2 font-weight-bold py-3">Sensor Details</th>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Data Source</th>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Location & Depth</th>
                                            <th class="text-subtitle-2 font-weight-bold py-3">Variables</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr v-for="sensor in sensors" :key="sensor.id">
                                            <td class="py-3">
                                                <div class="font-weight-bold text-subtitle-1">{{ sensor.id }}</div>
                                                <div v-if="sensor.note" class="text-caption text-medium-emphasis mt-1">
                                                    {{ sensor.note }}</div>
                                                <v-chip v-if="sensor.decommissioned" size="small" color="error"
                                                    variant="flat" class="mt-2 text-caption font-weight-bold">
                                                    <v-icon start size="14">mdi-alert-circle</v-icon>
                                                    Decommissioned
                                                </v-chip>
                                            </td>
                                            <td class="py-3">
                                                <div class="font-weight-medium">{{ sensor.organization }}</div>
                                                <div class="text-body-2">{{ sensor.contactName }}</div>
                                                <a :href="`mailto:${sensor.contactEmail}`"
                                                    class="text-body-2 text-decoration-none text-primary">{{
                                                    sensor.contactEmail }}</a>
                                            </td>
                                            <td class="py-3">
                                                <div class="d-flex align-center mb-1">
                                                    <v-icon size="16" class="mr-2"
                                                        color="medium-emphasis">mdi-map-marker</v-icon>
                                                    <span class="text-body-2">{{ sensor.coordinates }}</span>
                                                </div>
                                                <div class="d-flex align-center">
                                                    <v-icon size="16" class="mr-2"
                                                        color="medium-emphasis">mdi-arrow-down-box</v-icon>
                                                    <span class="text-body-2">{{ sensor.depth }}</span>
                                                </div>
                                            </td>
                                            <td class="py-3">
                                                <div class="text-body-2" style="max-width: 300px; line-height: 1.4;">{{
                                                    sensor.variables }}</div>
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
                        <v-card-text
                            class="d-flex flex-column flex-md-row align-center justify-space-between py-8 px-6">
                            <div class="mb-6 mb-md-0 text-center text-md-left">
                                <h3
                                    class="text-h4 font-weight-bold mb-3 d-flex align-center justify-center justify-md-start">
                                    <!-- <v-icon color="secondary" size="36" class="mr-3">mdi-email-fast</v-icon> -->
                                    Get in Touch
                                </h3>
                                <p class="text-body-1 text-medium-emphasis mb-0" style="max-width: 600px;">
                                    Have feedback or datasets you'd like to see incorporated into the Ocean
                                    Acidification & Hypoxia app? We'd love to hear from you.
                                </p>
                            </div>
                            <div class="d-flex flex-column flex-sm-row gap-4">
                                <v-btn prepend-icon="mdi-email" color="secondary" variant="flat" size="large"
                                    class="text-none font-weight-bold rounded-lg mx-2 my-2 my-sm-0"
                                    href="mailto:yayla.sezginer@cioospacific.ca">
                                    Yayla Sezginer
                                </v-btn>
                                <v-btn prepend-icon="mdi-email" color="secondary" variant="flat" size="large"
                                    class="text-none font-weight-bold rounded-lg mx-2 my-2 my-sm-0"
                                    href="mailto:taimazb@oceannetworks.ca">
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
    // contactName: string
    // contactEmail: string
    coordinates: string
    variables: string
    depth: string
    orgClass: 'org-hakai' | 'org-onc'
    dataUrl: string
    decommissioned?: boolean
}

const sensors: Sensor[] = [
    {
        id: 'Kwakshua Channel CO2 sensor',
        organization: 'Hakai Institute',
        // contactName: 'Hakai Institute',
        // contactEmail: 'wiley.evans@hakai.org',
        coordinates: '51.65, -127.97',
        variables: 'pCO2 (uatm), Temp (degC), Salinity (g/kg)',
        depth: 'Surface',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiKCBuoyResearch.html',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bamfield Marine Science Centre Burke-o-Lator',
        organization: 'Hakai Institute',
        // contactName: 'Hakai Institute',
        // contactEmail: 'wiley.evans@hakai.org',
        coordinates: '48.837, -125.136',
        variables: 'pCO2 (uatm), DIC (umol/kg), Temp (degC), Salinity (g/kg), Total Alkalinity (umol/kg), pH, Omega aragonite, Omega calcite',
        depth: '20m',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiBamfieldBoL5min.html',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bamfield Marine Science Centre PEARL Lab',
        organization: 'Ocean Networks Canada',
        // contactName: 'Ocean Networks Canada',
        // contactEmail: 'wiley.evans@hakai.org',
        coordinates: '48.837, -125.136',
        variables: 'Temp (degC), Salinity (g/kg), dO2 (mL/L)',
        depth: 'Surface',
        dataUrl: 'https://docs.google.com/document/d/1qHz_-IKc52c4snH105f8YSwqFDXGOETEzTEIZyTpgKM/edit?tab=t.0',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bute Inlet',
        organization: 'Hakai Institute',
        // contactName: 'Hakai Institute',
        coordinates: '50.60, -124.90',
        variables: 'pCO2 (uatm), Temp (degC), Salinity (g/kg)',
        depth: 'Profile',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiButeInletBuoy.html',
        orgClass: 'org-hakai'
    },
    {
        id: 'Quadra Island Hyacinthe Bay Burke-o-Lator',
        organization: 'Hakai Institute',
        // contactName: 'Hakai Institute',
        // contactEmail: 'wiley.evans@hakai.org',
        coordinates: '50.116, -125.222',
        variables: 'Temp (degC), Salinity (g/kg), pCO2 (uatm), pH, Total Alkalinity (umol/kg), DIC (umol/kg), Omega aragonite, Omega calcite',
        depth: 'Surface',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiQuadraBoLResearch.html',
        orgClass: 'org-hakai'
    },
    {
        id: 'Rivers Inlet',
        organization: 'Hakai Institute',
        // contactName: 'Hakai Institute',
        // contactEmail: 'wiley.evans@hakai.org',
        coordinates: '51.60, -127.53',
        variables: 'Temp (degC), Salinity (g/kg), dO2 (mL/L)',
        depth: '90m, 245m',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiRiversInletMooringResearch.html',
        orgClass: 'org-hakai'
    },
    {
        id: 'Folger Pinnacle',
        organization: 'Ocean Networks Canada',
        // contactName: 'Stef Mellon',
        // contactEmail: 'smellon@uvic.ca',
        coordinates: '48.81, -125.28',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '25m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?locationCode=FGPPN',
        orgClass: 'org-onc'
    },
    {
        id: 'Folger Deep',
        organization: 'Ocean Networks Canada',
        // contactName: 'Stef Mellon',
        // contactEmail: 'smellon@uvic.ca',
        coordinates: '48.8082916667, -125.2815',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '95m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?locationCode=FGPD',
        orgClass: 'org-onc'
    },
    {
        id: 'Baynes Sound Profiling Instrument',
        organization: 'Ocean Networks Canada',
        // contactName: 'Zarah Zheng',
        // contactEmail: 'zarahzheng@uvic.ca',
        coordinates: '49.487, -124.7693',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: 'Full water column',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?locationCode=BSPS',
        orgClass: 'org-onc'
    },
    {
        id: 'Baynes Sound Historical Mooring',
        organization: 'Ocean Networks Canada',
        // contactName: 'Zarah Zheng',
        // contactEmail: 'zarahzheng@uvic.ca',
        coordinates: '49.487, -124.7693',
        variables: 'pCO2 (uatm), O2 (mL/L), Temp',
        depth: '5, 20, 40m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?locationCode=BSM',
        orgClass: 'org-onc',
        decommissioned: true
    },
        {
        id: 'ORCA1-Twanoh',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.375, -123.00833333',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Profile',
        dataUrl: 'https://erddap.nanoos.org/erddap/griddap/orca_hydro_twanoh.html',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA2-Hoodsport',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.42181666, -123.11258333',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Profile',
        dataUrl: 'https://erddap.nanoos.org/erddap/griddap/orca_hydro_hoodsport.html',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA3-Hansville',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.90733333, -122.62708333',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Profile',
        dataUrl: 'https://erddap.nanoos.org/erddap/griddap/orca_hydro_hansville.html',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA4-DabobBay',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.80341666, -122.80291666',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Profile',
        dataUrl: 'https://erddap.nanoos.org/erddap/griddap/orca_hydro_dabobbay.html',
        orgClass: 'org-onc'
    },
    {
        id: 'PISCES1-South',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.665112, -122.873272',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Surface & Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/tabledap/pisces2_L1_profiles.html',
        orgClass: 'org-onc'
    },
    {
        id: 'PISCES2-North',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.692312, -122.865058',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Surface',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/tabledap/pisces2_surfctd.html',
        orgClass: 'org-onc'
    },
    {
        id: 'NPBY2-Carr Inlet',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.28, -122.73',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Profile',
        dataUrl: 'https://erddap.nanoos.org/erddap/griddap/orca_hydro_carrinlet.html',
        orgClass: 'org-onc'
    },
    {
        id: 'NPBY1-Pt Wells',
        organization: 'Northwest Environmental Moorings Group at University of Washington - Applied Physical Laboratory',
        coordinates: '47.76116666, -122.39716666',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU), Chlorophyll (mg/m3), NO3 (umol/kg)',
        depth: 'Profile',
        dataUrl: 'https://erddap.nanoos.org/erddap/griddap/orca_hydro_pointwells.html',
        orgClass: 'org-onc'
    },
    {
        id: 'Central Strait of Georgia platform',
        organization: 'Ocean Networks Canada',
        // contactName: 'Alice Bui',
        // contactEmail: 'aovbui@uvic.ca',
        coordinates: '49.05, -123.42',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '300m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?locationCode=SGC',
        orgClass: 'org-onc'
    },
    {
        id: 'Strait of Georgia East',
        organization: 'Ocean Networks Canada',
        // contactName: 'Alice Bui',
        // contactEmail: 'aovbui@uvic.ca',
        coordinates: '49.04, -123.32',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '170m',
        dataUrl:'https://data.oceannetworks.ca/DataSearch?locationCode=SGE',
        orgClass: 'org-onc'
    },
    {
        id: 'Scott2 - North Island Shelf',
        organization: 'IOS CTD mooring',
        coordinates: '51.12835, -129.47609',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '40m',
        dataUrl:'https://explore.cioos.ca/?lat=51.10935634566201&lon=-129.48571073830323&zoom=7.429494329687231&eovs=oxygen&platforms=mooring%2Cunknown&organizations=16%2C9%2C78%2C29%2C21%2C44&lang=en',
        orgClass: 'org-onc'
    },
    {
        id: 'E01- Southern Vancouver Island Shelf',
        organization: 'IOS CTD mooring',
        coordinates: '49.28333, -126.614685',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '35m',
        dataUrl:'',
        orgClass: 'org-onc'
    },
    {
        id: 'quat1-Quatsino Sound North Island',
        organization: 'IOS CTD mooring',
        coordinates: '50.41325, -128.00562',
        variables: 'Temp (K), dO2 (mL/L), Practical salinity (PSU)',
        depth: '40m',
        dataUrl:'',
        orgClass: 'org-onc'
    },
    {
        id: 'Amphirite Point',
        organization: 'BC Lightstation monitoring program (DFO)',
        coordinates: '48.9528, -125.5423',
        variables: 'Temp (K), Practical salinity (PSU)',
        depth: 'Surface',
        dataUrl:'https://open.canada.ca/data/en/dataset/719955f2-bf8e-44f7-bc26-6bd623e82884/resource/3226f487-d7a8-4e08-8bef-c6a1d87e7af3',
        orgClass: 'org-onc'
    },
    {
        id: 'Bonilla Island',
        organization: 'BC Lightstation monitoring program (DFO)',
        coordinates: '53.4928, -130.6358',
        variables: 'Temp (K), Practical salinity (PSU)',
        depth: 'Surface',
        dataUrl:'https://open.canada.ca/data/en/dataset/719955f2-bf8e-44f7-bc26-6bd623e82884/resource/3226f487-d7a8-4e08-8bef-c6a1d87e7af3',
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
