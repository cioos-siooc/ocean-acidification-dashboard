<template>
    <!-- BATHYMETRY -->
    <div class="source-card bg-elevated rounded-lg rounded-xl m-2 p-6">
        <div class="px-4 pt-4 pb-2 text-lg text-[22px] mt-4 mb-2">
            <a class="title-link" href="https://open.canada.ca/data/en/dataset/e6e11b99-f0cc-44f7-f5eb-3b995fb1637e" target="_blank"
                rel="noopener noreferrer">Canada West Coast Topo-Bathymetric Digital Elevation Model<UIcon name="i-mdi-open-in-new" class="size-4 ml-1.5 align-baseline opacity-70" /></a>
        </div>
        <p>This digital elevation model of Canada's west coast, developed by Natural Resources Canada in
            collaboration with the Department of Fisheries and Oceans, compiles the best data available from
            multiple government agencies into a seamless regional model gridded at 10 meter spacing, covering both
            marine bathymetric and adjacent terrestrial topographic areas.</p>
    </div>
</template>
<style scoped>
.source-card p + p {
    margin-top: 0.5rem;
}

.source-card a {
    color: var(--ui-primary);
    text-decoration: underline;
    text-decoration-color: color-mix(in oklab, currentColor 40%, transparent);
    text-underline-offset: 3px;
}

.source-card a:hover {
    text-decoration-color: currentColor;
}

.source-card a.title-link {
    text-decoration: none;
}

.source-card a.title-link:hover {
    text-decoration: underline;
}
</style>
