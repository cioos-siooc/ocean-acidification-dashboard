<template>
    <main class="grow min-h-0 bg-default">
        <!-- Hero Banner -->
        <div class="hero-banner text-white text-center py-16 px-4">
            <div class="mx-auto w-full max-w-screen-xl py-10">
                <h1 class="text-[57px] font-bold mb-6">Ocean Environmental Conditions Observer</h1>
                <!-- <p class="font-normal opacity-90 mb-0">
                    The Coastal Hypoxia, Ocean acidification, and 'Klimate' variables Evaluator.
                </p> -->
            </div>
        </div>

        <div class="mx-auto w-full max-w-screen-xl mt-n10 content-container pb-12">
            <div class="flex flex-wrap -m-3">
                <!-- Overview Card -->
                <div class="p-3 w-full">
                    <div class="bg-elevated rounded-lg rounded-xl p-2 mb-2">
                        <div class="text-muted p-6" style="line-height: 1.8;">
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
                                class="text-primary font-bold text-decoration-none">Canada's Ocean Acidification
                                Community of Practice</a> to discover our species impacts, action plans, Canada's expert
                            database and more.
                        </div>
                    </div>
                </div>

                <!-- Two Column Layout: Who We Are & User Guide -->
                <div class="p-3 w-full md:w-1/2">
                    <div class="bg-elevated rounded-lg rounded-xl h-full p-2">
                        <div class="text-lg font-bold flex items-center pb-4 pl-4 pt-4">
                            <!-- <UIcon name="i-mdi-account-multiple" class="size-[32px] text-success mr-4" /> -->
                            Who We Are
                        </div>
                        <div class="text-muted pl-4 pr-4" style="line-height: 1.6;">
                            The Ocean Environmental Conditions Observer app is developed and maintained by the Canadian Integrated Ocean Observing System 
                            (<a href="https://www.cioospacific.ca/" target="_blank" rel="noopener noreferrer"
                                class="text-primary text-decoration-none font-bold">CIOOS</a>)
                            Pacific regional association. OceanECO is designed to be a supporting tool for the "British Columbia Ocean Acidification and Hypoxia Action Plan" and "MEOPAR Ocean Acidification Community of Practice" partners. Data access is made possible by our data partners, which include Ocean Networks Canada, Hakai Institute, Fisheries and Oceans Canada, the University of British Columbia, and the University of Washington.
                        </div>
                    </div>
                </div>

                <div class="p-3 w-full md:w-1/2">
                    <div class="bg-elevated rounded-lg rounded-xl h-full p-2">
                        <div class="text-lg font-bold flex items-center pb-4 pl-4 pt-4">
                            <!-- <UIcon name="i-mdi-book-open-page-variant" class="size-[32px] text-warning mr-4" /> -->
                            User Guide
                        </div>
                        <div class="text-muted flex flex-col items-center justify-center pt-8">
                            <UIcon name="i-mdi-hammer-wrench" class="size-[48px] text-dimmed mb-4" />
                            <span class="font-light">Under construction - coming soon!</span>
                        </div>
                    </div>
                </div>

                <!-- Data Sources Card -->
                <div class="p-3 w-full mt-4">
                    <div class="bg-elevated rounded-lg rounded-xl p-6">
                        <div class="text-lg font-bold flex items-center pb-6">
                            <!-- <UIcon name="i-mdi-database-search" class="size-[36px] text-info mr-4" /> -->
                            Data Sources
                        </div>
                        <div class="pt-0">
                            <!-- External Data Sources -->
                            <div class="flex flex-wrap mb-8">
                                <div class="p-3 w-full">
                                    <div class="flex flex-col gap-6">
                                        <about-ssc />
                                        <about-nonna />
                                    </div>
                                </div>
                            </div>

                            <!-- Sensor Network -->
                            <USeparator class="mb-8" />
                            <h3 class="font-bold flex items-center mb-6 text-primary">
                                <UIcon name="i-mdi-access-point-network" class="mr-3" />
                                Sensor Network
                            </h3>

                            <div class="border border-default rounded-lg overflow-hidden">
                                <table class="w-full text-sm">
                                    <thead class="bg-elevated">
                                        <tr>
                                            <th class="text-subtitle-2 font-bold py-3">Sensor Details</th>
                                            <th class="text-subtitle-2 font-bold py-3">Data Source</th>
                                            <th class="text-subtitle-2 font-bold py-3">Location & Depth</th>
                                            <th class="text-subtitle-2 font-bold py-3">Variables</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr v-for="sensor in sensors" :key="sensor.id">
                                            <td class="py-3">
                                                <div class="font-bold text-subtitle-1">{{ sensor.id }}</div>
                                                <div v-if="sensor.note" class="text-muted mt-1">
                                                    {{ sensor.note }}</div>
                                                <UBadge size="sm" color="error" variant="solid" class="mt-2 font-bold rounded-full" v-if="sensor.decommissioned">
                                                    <UIcon name="i-mdi-alert-circle" class="size-[14px]" start />
                                                    Decommissioned
                                                </UBadge>
                                            </td>
                                            <td class="py-3">
                                                <div class="font-medium">{{ sensor.organization }}</div>
                                            </td>
                                            <td class="py-3">
                                                <div class="flex items-center mb-1">
                                                    <UIcon name="i-mdi-map-marker" class="size-[16px] text-muted mr-2" />
                                                    <span class="">{{ sensor.coordinates }}</span>
                                                </div>
                                                <div class="flex items-center">
                                                    <UIcon name="i-mdi-arrow-down-box" class="size-[16px] text-muted mr-2" />
                                                    <span class="">{{ sensor.depth }}</span>
                                                </div>
                                            </td>
                                            <td class="py-3">
                                                <div class="" style="max-width: 300px; line-height: 1.4;">{{
                                                    sensor.variables }}</div>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Sponsors Array -->
                <div class="p-3 w-full mt-12 mb-6">
                    <div class="text-center">
                        <h2 class="font-medium text-muted mb-8">Supported By</h2>
                        <div class="flex flex-wrap justify-center items-center mx-auto" style="max-width: 600px;">
                            <div class="p-3 w-full sm:w-1/2 flex justify-center">
                                <!-- <div class="bg-elevated rounded-lg flex flex-col items-center justify-center rounded-lg h-full w-full p-4" style="background-color: transparent;"> -->
                                <img src="/logos/MEOPAR.webp" alt="MEOPAR Logo" class="max-h-[80px] w-auto object-contain">
                                <!-- </div> -->
                            </div>
                            <div class="p-3 w-full sm:w-1/2 flex justify-center">
                                <!-- <div class="bg-elevated rounded-lg flex flex-col items-center justify-center rounded-lg h-full w-full p-4" style="background-color: transparent;"> -->
                                <img src="/logos/DFO.png" alt="DFO Logo" class="max-h-[80px] w-auto object-contain">
                                <!-- </div> -->
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Contact Us -->
                <div class="p-3 w-full">
                    <div class="bg-elevated rounded-lg rounded-xl p-2 bg-elevated contact-box">
                        <div class="flex flex-col md:flex-row items-center justify-between py-8 px-6">
                            <div class="mb-6 mb-md-0 text-center text-md-left">
                                <h3
                                    class="font-bold mb-3 flex items-center justify-center md:justify-start">
                                    <!-- <UIcon name="i-mdi-email-fast" class="size-[36px] text-secondary mr-3" /> -->
                                    Get in Touch
                                </h3>
                                <p class="text-muted mb-0" style="max-width: 600px;">
                                    Have feedback or datasets you'd like to see incorporated into the OceanECO app? We'd
                                    love to hear from you.
                                </p>
                            </div>
                            <div class="flex flex-col sm:flex-row gap-4">
                                <UButton variant="solid" size="lg" color="secondary" href="mailto:yayla.sezginer@cioospacific.ca" class="text-none font-bold rounded-lg mx-2 my-2 my-sm-0" leading-icon="i-mdi-email">
                                    Yayla Sezginer
                                </UButton>
                                <UButton variant="solid" size="lg" color="secondary" href="mailto:taimazb@oceannetworks.ca" class="text-none font-bold rounded-lg mx-2 my-2 my-sm-0" leading-icon="i-mdi-email">
                                    Taimaz Bahadory
                                </UButton>
                            </div>
                        </div>
                    </div>
                </div>

            </div>
        </div>
    </main>
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
