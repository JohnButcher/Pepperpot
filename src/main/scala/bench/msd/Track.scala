package bench.msd

import SumComp._

/**
 * Reproduced with permission from Austin Poulton from group BDP coursework, 2015
 * Case class and companion object that represents track data from the  million song database
 */

/**
 *
 * Note: confidence measures are bounded [0..1] on associated algorithmic estimated attributes
 * @param sampleRate
 * @param artist7Id
 * @param artistFamiliarity
 * @param artistHotness
 * @param artistId
 * @param artistLong
 * @param artistLocation
 * @param artistLat
 * @param artistMbid
 * @param artistName
 * @param artistPlaymeId
 * @param audioMd5
 * @param danceability
 * @param duration        song duration in
 * @param endOfFadeIn
 * @param energy          energy from a listener perspective ??
 * @param key             key the song is in
 * @param keyConfidence
 * @param loudness        overall loudness in dB
 * @param mode            major (1) or minor (0) key
 * @param modeConfidence
 * @param unknown         WHAT IS THIS?? All instances seem to have 1???
 * @param release         albumn name
 * @param release7Id
 * @param songHotness     algo est.
 * @param songId
 * @param startOfFadeOut
 * @param tempo           est. bpm - speed
 * @param timeSignature   est. # of beats per bar - rhythm
 * @param timeSignatureConfidence
 * @param title
 * @param trackId
 * @param track7Id
 * @param year
 */
class Track (val sampleRate : Int,
             val artist7Id  :  Int,
             val artistFamiliarity : Double,
             val artistHotness : Double,
             val artistId : String,
             val artistLong : Double,
             val artistLocation : String,
             val artistLat : Double,
             val artistMbid : String,
             val artistName : String,
             val artistPlaymeId : Int,
             val audioMd5 : String,
             val danceability : Double,
             val duration : Double,
             val endOfFadeIn : Double,
             val energy : Double,
             val key : Int,
             val keyConfidence : Double,
             val loudness : Double,
             val mode : Int,
             val modeConfidence : Double,
             val unknown : String,
             val release : String,
             val release7Id : Int,
             val songHotness : Double,
             val songId : String,
             val startOfFadeOut : Double,
             val tempo : Double,
             val timeSignature : Int,
             val timeSignatureConfidence : Double,
             val title : String,
             val trackId : Int,
             val track7Id : String,
             val year : Int)
{

  // song words to filter out
  final val filterSongWords : Set[String] = Set("a", "the", "you", "he", "she", "me", "i", "they", "yours", "there's")

  /**
    * derived feature: fade duration over track duration
    * @return
    */
  def fadeRatio: Double = ((this.duration-this.startOfFadeOut) + (this.danceability - this.endOfFadeIn))/this.duration



  //val song= (acc : SumComp,t:  Track) => spearmanAggregator(acc,t.tempo, t.songHotness)


    
  //def musicFeatureSet(): Vector[Double]  = ???

  /**
    * returns a list of filtered song words  - can be used by flat map to get song words by artists or accross the entire DB
    * @return
    */
  def songWords(): List[(String, Int)] =  {
    val songTitleLower = this.title.toLowerCase
    val words = songTitleLower.split(" ")
    val filteredWords = (for (w <- words if !filterSongWords.contains(w)) yield (w, 1)).toList
    filteredWords
  }


  def playmeIdPresent():Int = if (artistPlaymeId < 0) 0 else 1


  override def toString():String =
    "Artist:\t"+this.artistName+"\nSongTitle:\t"+this.title
}


object Track {

  /**
   *
   * @param t Array of strings representing the track meta data attributes
   * @throws java.lang.IllegalArgumentException - compatibility with Java
   * @throws java.lang.NumberFormatException
   * @return  A instance of Track
   */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[NumberFormatException])
  def apply(t :Array[String]):Option[Track] = {
    if (t.length != 34) None // throw new IllegalArgumentException("t corrupted: " + t.length)
    else {
      val track = new Track(
        toInt(t(0)).getOrElse(0),
        toInt(t(1)).getOrElse(0),
        toDouble(t(2)) getOrElse (0.0),
        toDouble(t(3)).getOrElse(0.0),
        t(4),
        toDouble(t(5)).getOrElse(0.0),
        t(6),
        toDouble(t(7)).getOrElse(0.0),
        t(8),
        t(9),
        toInt(t(10)).getOrElse(0),
        t(11),
        toDouble(t(12)).getOrElse(0.0),
        toDouble(t(13)).getOrElse(0.0),
        toDouble(t(14)).getOrElse(0.0),
        toDouble(t(15)).getOrElse(0.0),
        toInt(t(16)).getOrElse(0),
        toDouble(t(17)).getOrElse(0.0),
        toDouble(t(18)).getOrElse(0.0),
        toInt(t(19)).getOrElse(0),
        toDouble(t(20)).getOrElse(0.0),
        t(21),
        t(22),
        toInt(t(23)).getOrElse(0),
        toDouble(t(24)).getOrElse(0.0),
        t(25),
        toDouble(t(26)).getOrElse(0.0),
        toDouble(t(27)).getOrElse(0.0),
        toInt(t(28)).getOrElse(0),
        toDouble(t(29)).getOrElse(0.0),
        t(30),
        toInt(t(31)).getOrElse(0),
        t(32),
        toInt(t(33)).getOrElse(0)
      )
      Some(track) // return track
    }
  }

  /**
   * parses an Int from a string safely
   * @param s
   * @return
   */
  def toInt(s: String):Option[Int] = { try {
    Some(s.toInt) } catch {
    case e: NumberFormatException => None }
  }

  /**
   * parses a Double from a string safely
   * @param s
   * @return
   */
  def toDouble(s: String):Option[Double] = { try {
    Some(s.toDouble) } catch {
    case e: NumberFormatException => None }
  }

  /**
   * Parses a line from MSD CSV data files into a Track instance
   * @param t
   * @return
   */
  def createTrack(t: String) : Option[Track] =
  {
    val components = t.split( ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)
    //val components = t.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)

    val cleaned = components.map(s => s.replaceAll("\"",""))
    Track(cleaned)
    // TODO - discuss handling bad data - do we filter for None in collections or just create Dummy Tracks with certain key features that can be filtered?

  }
}
