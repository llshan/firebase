<template>
  <div class="schools_details">
    <div>
      <h1>School: {{ school }}</h1>
    </div>
  </div>
</template>

<script>
export default {
  name: 'SchoolDetails',

  data() {
    return {
      school: []
    }
  },

  mounted() {
    let url = 'https://us-central1-international-school-wikis.cloudfunctions.net/getSchool?';
    url += new URLSearchParams({ school_name: 'International School of Beijing' });
    fetch(url)
      .then(res => {console.log(res); return res.json()})
      .then(data => { this.school = data; console.log(data); })
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
