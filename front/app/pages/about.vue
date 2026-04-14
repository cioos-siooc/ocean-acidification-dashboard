<template>
    <v-main>
        <v-container>
            <v-card class="mx-auto mb-6 card-elevated">
                <v-card-title class="text-display-medium d-flex align-center gap-3">
                    <v-icon icon="mdi-information" size="36px" color="primary" class="mr-5"></v-icon>
                    About Us
                </v-card-title>
                <v-card-text class="pa-6">
                    <p>Welcome to CHOKE, the Coastal Hypoxia, Ocean acidification, and 'Klimate' variables
                        Evaluator.
                        The
                        coastal waters of British Columbia are becoming warmer, more acidic, and less oxygenated and
                        global
                        and
                        provincial emissions continue to rise. These shifts in the marine environment directly
                        affect
                        coastal
                        Canadian communities and industry. Accordingly, the BC Ocean Acidification and Hypoxia
                        Action
                        Plan
                        has
                        highlighted the strong need for public access to real-time information on dynamic essential
                        ocean
                        variables to enable adaptive decision making. CHOKE combines data from moored instruments
                        and
                        regional
                        ocean models to provide users with live maps and climatological records of ocean
                        temperature,
                        salinity,
                        pH, oxygen content, and the 'corrosiveness' of seawater to shelled organisms across southern
                        to
                        central
                        BC. To learn more, visit <a href="https://www.oceanacidification.ca/" target="_blank"
                            rel="noopener noreferrer">Canada's Ocean Acidification Community of Practice</a> to
                        discover
                        ocean
                        acidification and hypoxia species impacts, action plans, Canada's expert database and more.
                    </p>
                </v-card-text>
            </v-card>

            <v-card class="mx-auto mb-6 card-elevated">
                <v-card-title class="text-display-medium d-flex align-center gap-3">
                    <v-icon icon="mdi-account-multiple" size="36px" color="success" class="mr-5"></v-icon>
                    Who We Are
                </v-card-title>
                <v-card-text class="pa-6">
                    <p>CHOKE is developed and maintained by the Canadian Integrated Ocean Observing System (<a
                            href="https://www.cioospacific.ca/" target="_blank" rel="noopener noreferrer">CIOOS</a>)
                        Pacific
                        region in partnership with data providers, which include Ocean Networks Canada, Hakai
                        Institute,
                        Fisheries and Oceans Canada, the University of British Columbia, and the University of
                        Washington.
                        This
                        work is funded by The Marine Environmental Observation Prediction and Response Network (<a
                            href="https://www.meopar.ca/" target="_blank" rel="noopener noreferrer">MEOPAR</a>)
                    </p>
                </v-card-text>
            </v-card>

            <v-card class="mx-auto mb-6 card-elevated">
                <v-card-title class="text-display-medium d-flex align-center gap-3">
                    <v-icon icon="mdi-book-open" size="36px" color="warning" class="mr-5"></v-icon>
                    User Guide
                </v-card-title>
                <v-card-text class="pa-6">
                    <p>Under construction - coming soon!</p>
                </v-card-text>
            </v-card>

            <v-card class="mx-auto mb-6 card-elevated">
                <v-card-title class="text-display-medium d-flex align-center gap-3">
                    <v-icon icon="mdi-database" size="36px" color="info" class="mr-5"></v-icon>
                    Data Sources
                </v-card-title>
                <v-card-text class="pa-6">
                    <about-ssc />
                    <about-liveocean />
                    <about-nonna />

                    <!-- SENSORS -->
                    <v-card-title class="text-headline-small d-flex align-center gap-2 mt-6 mb-4">
                        <v-icon icon="mdi-wifi" size="24px" class="mr-5"></v-icon>
                        Sensor Network
                    </v-card-title>

                    <div class="sensor-table-wrapper">
                        <v-table density="compact" class="sensor-table">
                            <thead>
                                <tr class="table-header">
                                    <th>Sensor ID</th>
                                    <th>Point of Contact</th>
                                    <th>Geographic Coordinates</th>
                                    <th>Variables</th>
                                    <th>Depth</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="sensor in sensors" :key="sensor.id"
                                    :class="[sensor.orgClass, sensor.decommissioned ? 'org-decommissioned' : '']">
                                    <td>
                                        {{ sensor.id }}
                                        <br v-if="sensor.note">
                                        <span v-if="sensor.note" class="text-body-small">{{
                                            sensor.note }}</span>
                                        <v-chip v-if="sensor.decommissioned" size="x-small" color="error" label
                                            prepend-icon="mdi-alert" class="mt-1">Decommissioned</v-chip>
                                    </td>
                                    <td>
                                        <span class="org-name">{{ sensor.organization }}</span><br>
                                        {{ sensor.contactName }}<br>
                                        <a :href="`mailto:${sensor.contactEmail}`">{{ sensor.contactEmail }}</a>
                                    </td>
                                    <td>{{ sensor.coordinates }}</td>
                                    <td>{{ sensor.variables }}</td>
                                    <td>
                                        {{ sensor.depth }}
                                    </td>
                                </tr>
                            </tbody>
                        </v-table>
                    </div>

                </v-card-text>
            </v-card>

            <v-card class="mx-auto card-elevated">
                <v-card-title class="text-display-medium d-flex align-center gap-3">
                    <v-icon icon="mdi-email" size="36px" color="secondary" class="mr-5"></v-icon>
                    Contact Us
                </v-card-title>
                <v-card-text class="pa-6">
                    <p>Please contact <a href="mailto:yayla.sezginer@cioospacific.ca">Yayla Sezginer</a> and <a
                            href="mailto:taimazb@oceannetworks.ca">Taimaz Bahadory</a> with user experience
                        suggestions, general feedback, or datasets you would like to see incorporated into CHOKE.
                    </p>

                </v-card-text>
            </v-card>
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
