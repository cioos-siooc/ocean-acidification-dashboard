
<template>
  <svg xmlns="http://www.w3.org/2000/svg" width="14px" height="14px" viewBox="0 0 24 24">
  <defs>
    <linearGradient id="rainbow" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%"   stop-color="#ff8080"/>
      <stop offset="17%"  stop-color="#ffbb66"/>
      <stop offset="33%"  stop-color="#fff066"/>
      <stop offset="50%"  stop-color="#66dd66"/>
      <stop offset="67%"  stop-color="#66aaff"/>
      <stop offset="83%"  stop-color="#aa77ff"/>
      <stop offset="100%" stop-color="#cc66ff"/>
    </linearGradient>
  </defs>
  <path fill="url(#rainbow)" d="M17.45 17.55L12 23l-5.45-5.45l1.41-1.41L11 19.17V4.83L7.96 7.86L6.55 6.45L12 1l5.45 5.45l-1.41 1.41L13 4.83v14.34l3.04-3.03z"/>
</svg>
</template>

<script>
export default {
  name: 'MdiArrowUpDown'
}
</script>