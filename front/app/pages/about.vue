<template>
    <v-main class="bg-background">
        <!-- Hero Banner -->
        <v-sheet color="primary" class="hero-banner text-white text-center py-16 px-4" elevation="0">
            <v-container maxWidth="900" class="py-10">
                <h1 class="text-display-large font-weight-bold mb-6">Ocean Environmental Conditions Observer</h1>
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
                            The Ocean Environmental Conditions Observer app combines data from moored instruments and
                            regional
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
                            The Ocean Environmental Conditions Observer app is developed and maintained by the Canadian Integrated Ocean Observing System 
                            (<a href="https://www.cioospacific.ca/" target="_blank" rel="noopener noreferrer"
                                class="text-primary text-decoration-none font-weight-bold">CIOOS</a>)
                            Pacific regional association. OceanECO is designed to be a supporting tool for the "British Columbia Ocean Acidification and Hypoxia Action Plan" and "MEOPAR Ocean Acidification Community of Practice" partners. Data access is made possible by our data partners, which include Ocean Networks Canada, Hakai Institute, Fisheries and Oceans Canada, the University of British Columbia, and the University of Washington.
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
                                    Have feedback or datasets you'd like to see incorporated into the OceanECO app? We'd
                                    love to hear from you.
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
        id: 'Quadra Island Hyacinthe Bay Burke-o-Lator (Research)',
        organization: 'Hakai Institute',
        coordinates: '50.1160, -125.2220',
        variables: 'Omega aragonite, Omega calcite, pH, Salinity (PSU), Temp (degC)',
        depth: 'Surface',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiQuadraBoLResearch',
        orgClass: 'org-hakai'
    },
    {
        id: 'Kwakshua Channel CO2 Buoy (Provisional)',
        organization: 'Hakai Institute',
        coordinates: '51.6499, -127.9663',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '0.5m',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiKCBuoy1hour',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bute Inlet BIOOS Buoy (Research)',
        organization: 'Hakai Institute',
        coordinates: '50.5970, -124.8990',
        variables: 'pCO2 (uatm), Salinity (PSU), Temp (degC)',
        depth: '0.5m',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiBIOOSBuoyResearch',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bute Inlet BIOOS Wirewalker (Provisional)',
        organization: 'Hakai Institute',
        coordinates: '50.5744, -124.9009',
        variables: 'Depth (dbar), Dissolved O2 (mL/L), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiWirewalkerProvisional',
        orgClass: 'org-hakai'
    },
    {
        id: 'Bamfield Marine Sciences Centre Burke-o-Lator (Provisional)',
        organization: 'Hakai Institute',
        coordinates: '48.8366, -125.1363',
        variables: 'pH, Salinity (PSU), Temp (degC)',
        depth: '20m',
        dataUrl: 'https://catalogue.hakai.org/erddap/tabledap/HakaiBamfieldBoL5min',
        orgClass: 'org-hakai'
    },
    {
        id: '40mbss',
        organization: 'Ocean Networks Canada',
        coordinates: '49.4869, -124.7693',
        variables: 'Dissolved O2 (mL/L), pH, Salinity (PSU), Temp (degC)',
        depth: '41m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BSM.J3',
        orgClass: 'org-onc'
    },
    {
        id: 'Hartley Bay Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '53.4223, -129.2468',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '80m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=HBIP',
        orgClass: 'org-onc'
    },
    {
        id: 'China Creek Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '49.1538, -124.8024',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '110m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=CCIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Red Island Shoal Buoy',
        organization: 'Ocean Networks Canada',
        coordinates: '47.3104, -54.0784',
        variables: 'Temp (degC)',
        depth: 'Surface',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=RISB',
        orgClass: 'org-onc'
    },
    {
        id: 'Upper Slope South',
        organization: 'Ocean Networks Canada',
        coordinates: '48.4269, -126.1746',
        variables: 'Temp (degC)',
        depth: '394m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BACUS',
        orgClass: 'org-onc'
    },
    {
        id: 'Bullseye',
        organization: 'Ocean Networks Canada',
        coordinates: '48.6706, -126.8480',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '1257m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=NC89',
        orgClass: 'org-onc'
    },
    {
        id: 'Campbell River Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '50.0208, -125.2354',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '8.2m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=CRIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Instrument Float',
        organization: 'Ocean Networks Canada',
        coordinates: '48.6224, -123.4988',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '0.1m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=YPVPF',
        orgClass: 'org-onc'
    },
    {
        id: 'Folger Deep',
        organization: 'Ocean Networks Canada',
        coordinates: '48.8138, -125.2806',
        variables: 'Salinity (PSU), Temp (degC)',
        depth: '96m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=FGPD',
        orgClass: 'org-onc'
    },
    {
        id: 'Folger Pinnacle',
        organization: 'Ocean Networks Canada',
        coordinates: '48.8083, -125.2815',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '25m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=FGPPN',
        orgClass: 'org-onc'
    },
    {
        id: 'Saanich Inlet Sill',
        organization: 'Ocean Networks Canada',
        coordinates: '48.6887, -123.5002',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '88m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=SILL',
        orgClass: 'org-onc'
    },
    {
        id: 'Holyrood Bay Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '47.4258, -53.1211',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), pH, Salinity (PSU), Temp (degC)',
        depth: '84m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=HRBIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Wally Land',
        organization: 'Ocean Networks Canada',
        coordinates: '48.3118, -126.0656',
        variables: 'Chlorophyll (ug/L), Salinity (PSU), Temp (degC)',
        depth: '863m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BACWL',
        orgClass: 'org-onc'
    },
    {
        id: 'Barkley Canyon Mid-West',
        organization: 'Ocean Networks Canada',
        coordinates: '48.3151, -126.0588',
        variables: 'Temp (degC)',
        depth: '891m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BACMW',
        orgClass: 'org-onc'
    },
    {
        id: 'Microsquid Digital Camera Frame',
        organization: 'Ocean Networks Canada',
        coordinates: '48.6509, -123.4873',
        variables: 'Salinity (PSU), Temp (degC)',
        depth: '101m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=PDCF',
        orgClass: 'org-onc'
    },
    {
        id: 'Strait of Georgia East VENUS Instrument Platform',
        organization: 'Ocean Networks Canada',
        coordinates: '49.0426, -123.3170',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '167m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=SEVIP',
        orgClass: 'org-onc'
    },
    {
        id: 'St. John\'s Buoy',
        organization: 'Ocean Networks Canada',
        coordinates: '47.5668, -52.6303',
        variables: 'Temp (degC)',
        depth: 'Surface',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=STJB',
        orgClass: 'org-onc'
    },
    {
        id: 'Kitamaat Village Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '53.9747, -128.6571',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '45m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=KVIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Port aux Basques Buoy',
        organization: 'Ocean Networks Canada',
        coordinates: '47.5499, -59.0742',
        variables: 'Temp (degC)',
        depth: 'Surface',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=PABB',
        orgClass: 'org-onc'
    },
    {
        id: 'Main Endeavour Field',
        organization: 'Ocean Networks Canada',
        coordinates: '47.9488, -129.0984',
        variables: 'Temp (degC)',
        depth: '2190m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=KEMF',
        orgClass: 'org-onc'
    },
    {
        id: 'ODP 1026',
        organization: 'Ocean Networks Canada',
        coordinates: '47.7626, -127.7586',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '2658m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=NC27',
        orgClass: 'org-onc'
    },
    {
        id: 'Macaulay Outfall Mooring',
        organization: 'Ocean Networks Canada',
        coordinates: '48.4011, -123.4088',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '56m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=MAC',
        orgClass: 'org-onc'
    },
    {
        id: 'Bottom Boundary Layer',
        organization: 'Ocean Networks Canada',
        coordinates: '49.0807, -123.3395',
        variables: 'Salinity (PSU), Temp (degC)',
        depth: '145m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=LSBBL',
        orgClass: 'org-onc'
    },
    {
        id: 'Barkley Canyon Mid-East',
        organization: 'Ocean Networks Canada',
        coordinates: '48.3149, -126.0583',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '895m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BACME',
        orgClass: 'org-onc'
    },
    {
        id: '20mbss',
        organization: 'Ocean Networks Canada',
        coordinates: '49.4869, -124.7693',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '21m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BSM.J2',
        orgClass: 'org-onc'
    },
    {
        id: 'JF2C Mooring',
        organization: 'Ocean Networks Canada',
        coordinates: '48.3567, -124.2154',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '182m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=JF2C',
        orgClass: 'org-onc'
    },
    {
        id: 'Barkley Upper Slope',
        organization: 'Ocean Networks Canada',
        coordinates: '48.4273, -126.1745',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '397m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=NCBC',
        orgClass: 'org-onc'
    },
    {
        id: 'AS04 Mooring',
        organization: 'Ocean Networks Canada',
        coordinates: '48.3007, -123.3911',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '113m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=AS04',
        orgClass: 'org-onc'
    },
    {
        id: '5mbss',
        organization: 'Ocean Networks Canada',
        coordinates: '49.4869, -124.7693',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '4.9m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BSM.J1',
        orgClass: 'org-onc'
    },
    {
        id: 'Boundary Pass Mooring',
        organization: 'Ocean Networks Canada',
        coordinates: '48.7661, -123.0396',
        variables: 'Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '223m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BDYPM',
        orgClass: 'org-onc'
    },
    {
        id: 'Burrard Inlet Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '49.3010, -123.1111',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), pH, Salinity (PSU), Temp (degC)',
        depth: '28m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BIIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Barkley Canyon Axis',
        organization: 'Ocean Networks Canada',
        coordinates: '48.3167, -126.0505',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '984m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=BACAX',
        orgClass: 'org-onc'
    },
    {
        id: 'Strait of Georgia VENUS Instrument Platform',
        organization: 'Ocean Networks Canada',
        coordinates: '49.0398, -123.4257',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), Salinity (PSU), Temp (degC)',
        depth: '299m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=SCVIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Cambridge Bay Underwater Network',
        organization: 'Ocean Networks Canada',
        coordinates: '69.1131, -105.0636',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mL/L), pH, Salinity (PSU), Temp (degC)',
        depth: '8.5m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=CBYIP',
        orgClass: 'org-onc'
    },
    {
        id: 'Saanich Inlet VENUS Instrument Platform',
        organization: 'Ocean Networks Canada',
        coordinates: '48.6513, -123.4864',
        variables: 'Temp (degC)',
        depth: '96m',
        dataUrl: 'https://data.oceannetworks.ca/DataSearch?location=PVIP',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA - Hoodsport',
        organization: 'University of Washington, NANOOS-IOOS, Washington Ocean Acidification Center',
        coordinates: '47.4218, -122.6126',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mg/L), Salinity (PSU), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/orca2_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA - Dabob Bay',
        organization: 'University of Washington, NANOOS-IOOS, Washington Ocean Acidification Center',
        coordinates: '47.8034, -122.8029',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mg/L), Salinity (PSU), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/orca4_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA - Point Wells',
        organization: 'University of Washington, NANOOS-IOOS, Washington Ocean Acidification Center',
        coordinates: '47.7612, -122.3972',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mg/L), Salinity (PSU), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/npby1_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'PISCES1-South (Surface Hydrological Station)',
        organization: 'University of Washington, Applied Physics Laboratory',
        coordinates: '47.6651, -122.8733',
        variables: 'Chlorophyll (ug/L), pH, Salinity (PSU), Temp (degC), Dissolved O2 (mg/L)',
        depth: 'Surface',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/tabledap/pisces1_surfctd',
        orgClass: 'org-onc'
    },
    {
        id: 'PISCES2-North (Depth-Gridded Profile)',
        organization: 'University of Washington, Applied Physics Laboratory',
        coordinates: '47.6923, -122.8651',
        variables: 'Chlorophyll (ug/L), Salinity (PSU), Temp (degC), Dissolved O2 (mg/L)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/pisces2_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'PISCES2-North (Surface Hydrological Station)',
        organization: 'University of Washington, Applied Physics Laboratory',
        coordinates: '47.6923, -122.8651',
        variables: 'Chlorophyll (ug/L), pH, Salinity (PSU), Temp (degC), Dissolved O2 (mg/L)',
        depth: 'Surface',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/tabledap/pisces2_surfctd',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA - Hansville',
        organization: 'University of Washington, NANOOS-IOOS, Washington Ocean Acidification Center',
        coordinates: '47.9075, -122.6274',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mg/L), Salinity (PSU), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/orca3_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA - Carr Inlet',
        organization: 'University of Washington, NANOOS-IOOS, Washington Ocean Acidification Center',
        coordinates: '47.2800, -122.7300',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mg/L), Salinity (PSU), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/npby2_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'PISCES1-South (Depth-Gridded Profile)',
        organization: 'University of Washington, Applied Physics Laboratory',
        coordinates: '47.6651, -122.8733',
        variables: 'Salinity (PSU), Temp (degC), Chlorophyll (ug/L), Dissolved O2 (mg/L)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/pisces1_L3_depthgridded_025',
        orgClass: 'org-onc'
    },
    {
        id: 'ORCA - Twanoh',
        organization: 'University of Washington, NANOOS-IOOS, Washington Ocean Acidification Center',
        coordinates: '47.3750, -123.0083',
        variables: 'Chlorophyll (ug/L), Dissolved O2 (mg/L), Salinity (PSU), Temp (degC)',
        depth: 'Profile',
        dataUrl: 'https://nwem.apl.uw.edu/erddap/griddap/orca1_L3_depthgridded_025',
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
