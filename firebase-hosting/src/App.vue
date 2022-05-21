<template>
  <Maps />
  <Schools />
  <SchoolDetails />
</template>

<script>
import Maps from './components/Maps.vue';
import Schools from './components/Schools.vue';
import SchoolDetails from './components/SchoolDetails.vue';

export default {
  name: 'App',
  components: {
    Maps,
    Schools,
    SchoolDetails
  }
}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>