import nimrod._
import nimrod.tasks._

mkdir("moses") exec

wget("http://giza-pp.googlecode.com/files/giza-pp-v1.0.7.tar.gz") > "moses/giza-pp-v1.0.7.tar.gz" exec

tarxz("moses/giza-pp-v1.0.7.tar.gz") exec
