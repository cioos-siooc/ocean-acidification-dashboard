<template>
    <!-- SALISH SEA CAST -->
    <div class="source-card bg-elevated rounded-lg rounded-xl m-2 p-6">
        <div class="px-4 pt-4 pb-2 text-lg text-[22px] mt-4 mb-2">
            <a class="title-link" href="https://salishsea.eos.ubc.ca/nemo/" target="_blank" rel="noopener noreferrer">SalishSeaCast NEMO
                Model<UIcon name="i-mdi-open-in-new" class="size-4 ml-1.5 align-baseline opacity-70" /></a>
        </div>
        <p>Operated by the SalishSeaCast Project Team at the University of British Columbia. (Point of
            Contact:
            <a href="https://www.eoas.ubc.ca/people/susanallen" target="_blank" rel="noopener noreferrer">Dr.
                Susan Allen</a>).
        </p>

        <p>Cite as:</p>
        <p class="italic">
            Jarníková T., Ianson D., Allen S.E., Shao A.E., Olson E.M.. 2022. Anthropogenic Carbon
            Increase
            has
            Caused Critical Shifts in Aragonite Saturation Across a Sensitive Coastal System. Global
            Biogeochemical Cycles, 36(7). <a href="https://doi.org/10.1029/2021GB007024" target="_blank"
                rel="noopener noreferrer">https://doi.org/10.1029/2021GB007024</a>
        </p>
        <p class="italic">
            Moore-Maley, B. L., S. E. Allen, and D. Ianson, 2016. Locally-driven interannual variability
            of
            near-surface pH and ΩA in the Strait of Georgia. J. Geophys. Res. Oceans, 121(3), 1600–1625.
            <a href="https://dx.doi.org/10.1002/2015JC01111" target="_blank"
                rel="noopener noreferrer">https://dx.doi.org/10.1002/2015JC01111</a>
        </p>
        <p>
            <a href="https://github.com/SalishSeaCast/docs/blob/main/CITATION.rst" target="_blank"
                rel="noopener noreferrer">more</a>
        </p>
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
