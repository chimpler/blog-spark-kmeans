package com.chimpler.example.kmeans

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.awt._
import be.humphreys.simplevoronoi.{GraphEdge, Voronoi}
import scala.collection.JavaConversions._
import scala.collection.mutable
import java.util.Locale
import java.text.NumberFormat

object KMeansApp extends App {
  val backgroundImageFileName = "world-map.png"
  val imageWidth = 1000
  val imageHeight = 500

  private def toImageCoordinates(longitude: Double, latitude: Double, imageWidth: Int, imageHeight: Int): (Int, Int) = {
    (
      (imageWidth * (0.5 + longitude / 360)).toInt,
      (imageHeight * (0.5 - latitude / 180)).toInt
      )
  }

  private def generateColor(group: Int, groupCount: Int): Color = {
    val hue = group.toFloat / groupCount
    val saturation = 0.8f
    val lightness = 0.5f
    Color.getHSBColor(hue, saturation, lightness)
  }

  private def drawMapBackground() {
    val mapBackground = ImageIO.read(KMeansApp.getClass.getClassLoader.getResourceAsStream(backgroundImageFileName))
    graphics.drawImage(mapBackground, 0, 0, imageWidth, imageHeight, Color.WHITE, null)
    graphics.setColor(new Color(0, 0, 0, 100))
    graphics.fillRect(0, 0, imageWidth, imageHeight)
  }

  private def computeVoronoiCells():Array[mutable.Buffer[(Double, Double)]] = {

    val xValuesIn = clusterCenters map (_(0))
    val yValuesIn = clusterCenters map (_(1))

    val voronoi = new Voronoi(0.0001)
    val graphEdges = voronoi.generateVoronoi(xValuesIn, yValuesIn, -180d, 180d, -90d, 90d)

    val voronoiCellByGroup = new Array[mutable.Buffer[(Double, Double)]](clusterCount)
    val edgeBySite = new Array[mutable.Buffer[GraphEdge]](clusterCount)
    for (i <- 0 until clusterCount) edgeBySite(i) = mutable.Buffer.empty[GraphEdge]

    for (edge <- graphEdges) {
      // skip edge with 0 length (bug in voronoi lib?)
      if (edge.x1 != edge.x2 || edge.y1 != edge.y2) {
        edgeBySite(edge.site1) += edge
        edgeBySite(edge.site2) += edge
      }
    }

    for (group <- 0 until clusterCount) {
      val points = edgeBySite(group) flatMap { edge => Array((edge.x1, edge.y1), (edge.x2, edge.y2))}

      val centerX = clusterCenters(group)(0)
      val centerY = clusterCenters(group)(1)

      // sort the points by ascending angle (between the x-axis and the point)
      val sortedPoints = points.sortBy {
        case (px, py) =>
          math.atan2(py - centerY, -px + centerX)
      }

      val completedPoints = mutable.Buffer.empty[(Double, Double)]

      for (i <- 0 until sortedPoints.size) {
        val point = sortedPoints(i)
        val nextPoint = sortedPoints((i + 1) % sortedPoints.size)
        completedPoints += point

        (point._1, point._2, nextPoint._1, nextPoint._2) match {
          case (_, -90d, -180d, _) => // missing bottom-left corner
            completedPoints += ((-180d, -90d))
          case (180d, _, _, -90d) => // missing bottom-right corner
            completedPoints += ((180d, -90d))
          case (_, 90d, 180d, _) => // missing top-left corner
            completedPoints += ((180d, 90d))
          case (-180d, _, _, 90d) => // missing top-right corner
            completedPoints += ((-180d, 90d))
          case (_, -90d, _, 90d) => // missing bottom-left corner and top-left corner
            completedPoints += ((-180d, -90d))
            completedPoints += ((-180d, 90d))
          case (-180d, _, 180d, _) => // missing top-left corner and top-right corner
            completedPoints += ((-180d, 90d))
            completedPoints += ((180d, 90d))
          case (_, 90d, _, -90d) => // missing top-right corner and bottom-right corner
            completedPoints += ((180d, 90d))
            completedPoints += ((180d, -90d))
          case (180d, _, -180d, _) => // missing bottom-right corner and bottom-left corner
            completedPoints += ((180d, -90d))
            completedPoints += ((-180d, -90d))
          case _ =>
        }
      }

      voronoiCellByGroup(group) = completedPoints
    }

    voronoiCellByGroup
  }

  private def drawVoronoiCells() {
    // draw voronoi cells
    graphics.setStroke(new BasicStroke(2))
    for(group <- 0 until clusterCount) {
      val points = voronoiCellByGroup(group)

      val polygon = new Polygon()
      for((px, py) <- points) {
        val (imgX, imgY) = toImageCoordinates(px, py, imageWidth, imageHeight)
        polygon.addPoint(imgX, imgY)
      }

      val color = groupColors(group)
      val alphaColor = new Color(color.getRed, color.getGreen, color.getBlue, 60)
      graphics.setColor(alphaColor)
      graphics.fillPolygon(polygon)

      graphics.setColor(new Color(255, 255, 255, 100))
      graphics.drawPolygon(polygon)
    }
  }

  private def drawPercentileCircles() {
    for ((group, tweets) <- tweetsByGoup) {
      val clusterCenter = clusterCenters(group)

      val distances = tweets
        .map {
        case coordinates: Array[Double] =>
          val dx = clusterCenter(0) - coordinates(0)
          val dy = clusterCenter(1) - coordinates(1)
          Math.sqrt(dx * dx + dy * dy)
        }
        .toList
      distances.sortBy(_.toDouble)

      val tweetCount = tweets.size
      val percentile50 = distances((tweetCount * 0.50d).toInt)
      val percentile90 = distances((tweetCount * 0.90d).toInt)


      val color = groupColors(group).brighter()
      val (x, y) = toImageCoordinates(clusterCenters(group)(0), clusterCenters(group)(1), imageWidth, imageHeight)

      // draw circle for percentiles
      val radius50 = (percentile50 * imageWidth / 180).toInt
      val radius90 = (percentile90 * imageWidth / 180).toInt

      val polygon = new Polygon()
      val points = voronoiCellByGroup(group)
      for((px, py) <- points) {
        val (imgX, imgY) = toImageCoordinates(px, py, imageWidth, imageHeight)
        polygon.addPoint(imgX, imgY)
      }
      graphics.clip(polygon)

      val alphaColor = new Color(color.getRed, color.getGreen, color.getBlue, 40)
      graphics.setColor(alphaColor)
      graphics.fillOval(x - radius50, y - radius50, radius50 * 2, radius50 * 2)
      graphics.fillOval(x - radius90, y - radius90, radius90 * 2, radius90 * 2)

      val borderColor = new Color(color.getRed, color.getGreen, color.getBlue, 200).darker()
      graphics.setColor(borderColor)
      graphics.drawOval(x - radius50, y - radius50, radius50 * 2, radius50 * 2)
      graphics.drawOval(x - radius90, y - radius90, radius90 * 2, radius90 * 2)

      graphics.setClip(0, 0, imageWidth, imageHeight)
    }
  }

  private def drawTweets() {
    for ((group, tweets) <- tweetsByGoup) {
      val color = groupColors(group).brighter()
      graphics.setColor(new Color(color.getRed, color.getGreen, color.getBlue, 50))
      for (coordinate <- tweets) {
        val (x, y) = toImageCoordinates(coordinate(0), coordinate(1), imageWidth, imageHeight)
        graphics.fillOval(x - 1, y - 1, 2, 2)
      }
    }
  }

  private def drawTweetCounts() {
    graphics.setColor(Color.WHITE)
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    val font = new Font(Font.SANS_SERIF, Font.BOLD, 18)
    graphics.setFont(font)
    for ((group, tweets) <- tweetsByGoup) {
      val tweetCount = numberFormat.format(tweets.size)
      val bound = font.getStringBounds(tweetCount, graphics.getFontRenderContext)
      val (x, y) = toImageCoordinates(clusterCenters(group)(0), clusterCenters(group)(1), imageWidth, imageHeight)
      // draw text shadow
      graphics.setColor(Color.BLACK)
      graphics.drawString(tweetCount, (x - bound.getWidth / 2).toInt + 1, (y + bound.getHeight + 10).toInt + 1)
      // draw text
      graphics.setColor(Color.WHITE)
      graphics.drawString(tweetCount, (x - bound.getWidth / 2).toInt, (y + bound.getHeight + 10).toInt)
    }
  }

  private def drawClusterCenters() {
    for (group <- 0 until clusterCount) {
      val (x, y) = toImageCoordinates(clusterCenters(group)(0), clusterCenters(group)(1), imageWidth, imageHeight)

      // draw center circle
      val color = groupColors(group)
      graphics.setColor(color)
      graphics.fillOval(x - 6, y - 6, 12, 12)
      graphics.setColor(Color.WHITE)
      graphics.drawOval(x - 6, y - 6, 12, 12)
    }
  }


  if (args.length < 2) {
    sys.error("Arguments: <tweet_file> <output_image_file>")
  }

  val tweetFilename = args(0)
  val imageFilename = args(1)

  val sc = new SparkContext("local[4]", "kmeans")

  // Load and parse the data
  val data = sc.textFile(tweetFilename)
  val parsedData = data.map {
    line =>
      Vectors.dense(line.split(',').slice(0, 2).map(_.toDouble))
  }

  // Cluster the data into five classes using KMeans
  val iterationCount = 100
  val clusterCount = 8
  val model = KMeans.train(parsedData, clusterCount, iterationCount)
  val clusterCenters = model.clusterCenters map (_.toArray)

  val cost = model.computeCost(parsedData)
  println("Cost: " + cost)

  val tweetsByGoup = data
    .map {_.split(',').slice(0, 2).map(_.toDouble)}
    .groupBy {rdd => model.predict(Vectors.dense(rdd))}
    .collect()
  sc.stop()

  // create image
  val image = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB)
  val graphics = image.createGraphics
  graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

  val groupColors = for (group <- 0 until clusterCount) yield generateColor(group, clusterCount)
  val voronoiCellByGroup = computeVoronoiCells()

  // draw map
  drawMapBackground()

  drawTweets()

  drawVoronoiCells()

  drawPercentileCircles()

  drawClusterCenters()

  drawTweetCounts()

  // write image to disk
  ImageIO.write(image, "png", new File(imageFilename))
}
