<template>
    <!-- LIVE OCEAN -->
    <div class="source-card bg-elevated rounded-lg rounded-xl m-2 p-6">
        <div class="px-4 pt-4 pb-2 text-lg text-[22px] mt-4 mb-2">
            <a class="title-link" href="https://faculty.washington.edu/pmacc/LO/LiveOcean.html" target="_blank"
                rel="noopener noreferrer">LiveOcean Model<UIcon name="i-mdi-open-in-new" class="size-4 ml-1.5 align-baseline opacity-70" /></a>
        </div>
        <p>Operated by the University of Washington Coastal Modeling Group. (Point of Contact:
            <a href="mailto:kmhewett@uw.edu">Kate Hewett</a>).
        </p>

        <p>
            A daily 3-day forecast at 4-hourly steps, sliced at fixed depths (surface, 10&ndash;2500 m and
            the bottom layer). Each new run replaces the previous runs' forecast hours; past days hold each
            run's first day. The public archive starts in July 2025 (with only surface&ndash;50 m and bottom
            until late December 2025), so long-term views such as climatologies and trends aren't available
            for this model.
        </p>
        <p>
            Temperature and salinity are converted to Conservative Temperature and Absolute Salinity
            (TEOS-10) to match SalishSeaCast. Total alkalinity, DIC and &Omega; calcite are derived here
            from the model's pH and &Omega; aragonite with PyCO2SYS; they are not LiveOcean outputs.
        </p>

        <p>Cite as:</p>
        <p class="italic">
            MacCready, P., Xiong, J., Carini, R., &amp; Newton, J. (2025). LiveOcean. Benefits of Ocean
            Observing Catalog (BOOC), 3(1).
            <a href="https://doi.org/10.15351/3068-2320.1117" target="_blank"
                rel="noopener noreferrer">https://doi.org/10.15351/3068-2320.1117</a>
        </p>
        <p class="italic">
            MacCready, P., McCabe, R. M., Siedlecki, S. A., Lorenz, M., Giddings, S. N., Bos, J., Albertson,
            S., Banas, N. S., &amp; Garnier, S. (2021). Estuarine Circulation, Mixing, and Residence Times in
            the Salish Sea. Journal of Geophysical Research: Oceans, 126(2).
            <a href="https://doi.org/10.1029/2020jc016738" target="_blank"
                rel="noopener noreferrer">https://doi.org/10.1029/2020jc016738</a>
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
