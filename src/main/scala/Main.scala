import org.apache.hadoop.fs.{ FileSystem, Path,FSDataInputStream}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration



object Main extends App {

  val conf = new Configuration()
//  conf.addResource(new Path("/User/dhrumil/opt/hadoop-2.7.3/opt/cloudera/hdfs-site.xml"))
//  conf.addResource(new Path("/User/dhrumil/opt/hadoop-2.7.3/opt/cloudera/hdfs-site.xml"))

    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)

  println(fs.getUri) ///// Number 1
  println(fs.getWorkingDirectory)
  println("=======================================")
  fs
    .listStatus(new Path("/user/fall2019/dhrumil")) ///////////////////////////////// Number 2
    .map(_.getPath)
    .foreach(println)
  println("========================================")
  println(fs.getUri) /////////// Number 3

  println("========================================")

  if (fs.exists(new Path("/user/fall2019/dhrumil"))) ///////////////////   Number 4.a
  {
    println("Yay! I've found my folder! :)")
  }
  else {
    println("Nope, I failed in the previous practice! :(")
  }
fs.delete((new Path("/user/fall2019/dhrumilAssignment")))
  /////////// Number 4.b

  fs.mkdirs(new Path("/user/fall2019/dhrumil"))
/////// Number 4.c
  if (fs.exists(new Path("/user/fall2019/dhrumil")))
       println("Folder Created")
  else println("Unable to create the folder")
///////////// Number 4.d
  println("========================================")

  fs.mkdirs(new Path("/user/fall2019/dhrumil/stm"))
  if (fs.exists(new Path("/user/fall2019/dhrumil/stm")))
       println("Folder Created")
  else println("Unable to create the folder")
//////////// Number 4.e
  println("========================================")

  fs.copyFromLocalFile(new Path("/home/bd-user/Desktop/gtfs_stm/stops.txt"), new Path("/user/fall2019/dhrumil/stm"))
 //////////// Number 4.f
  fs.copyFromLocalFile(new Path("/home/bd-user/Desktop/gtfs_stm/stops.txt"), new Path("/user/fall2019/dhrumil/stm/stops2.txt"))
  ///////////// Number 4.g


  fs.rename(new Path("/user/fall2019/dhrumil/stm/stops2.txt"), new Path("/user/fall2019/dhrumil/stm/stops.csv"))
////////////// Number 4.h

  val inputStream: FSDataInputStream = fs.open(new Path("/user/fall2019/dhrumil/stm/stops.csv"))
  val data: String = IOUtils.toString(inputStream)
  data.split("\n").slice(0, 5).foreach(println)

////////////////////// Number 4.i
}
