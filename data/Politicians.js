  
var mongoose = require("mongoose"),
Schema = mongoose.Schema;

var model = new Schema(
{
  politics_cpf: String,  
  politics_data: Object,
  personal_data: Object,
  nada_consta: Object,
  policia_civil: Object,
  policia_federal: Object
},
{
  strict: false,
}
);

module.exports = mongoose.model("Politicians", model);