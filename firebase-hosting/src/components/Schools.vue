<template>
  <div class="schools">
    <div>
      <h1>List of Schools: {{ schools }}</h1>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Schools',

  data() {
    return {
      schools: []
    }
  },

  mounted() {
    fetch('https://us-central1-international-school-wikis.cloudfunctions.net/getSchoolList')
      .then(res => {console.log(res); return res.json()})
      .then(data => { this.schools = data; console.log(data); })
      .catch(err => console.log(err.message))
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>
