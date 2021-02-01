// Function is dedicated by Mozilla to the Public Domain. http://creativecommons.org/publicdomain/zero/1.0/
//
// Return random integer - https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Math/random#Getting_a_random_integer_between_two_values_inclusive
//
function getRandomIntInclusive(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  let rnd = -1 * Math.floor(Math.random() * (max - min + 1) + min);
  return rnd;
}

module.exports = {
  getRandomIntInclusive: getRandomIntInclusive
}
