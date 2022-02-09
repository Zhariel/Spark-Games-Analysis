import java.io.File

class Utils {

  def getListOfFiles(dir: File):List[File] = dir.listFiles.filter(_.isFile).toList

}
