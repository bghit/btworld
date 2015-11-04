/**
 * Created by Bogdan Ghit on 11/2/15.
 * 
 */


class TopKRank extends java.io.Serializable {

  var queue = new MyPriorityQueue[Scrape](Utils.TOP_K_RECORDS)

  def insert(s: Scrape) = {
    queue += s
  }

  def merge(top1: TopKRank, top2: TopKRank): TopKRank = {
    var top = new TopKRank

    val array1 = top1.queue.toArray
    for (x <- 0 to array1.size-1) {
      top.insert(array1(x))
    }

    val array2 = top2.queue.toArray
    for (x <- 0 to array2.size-1) {
      top.insert(array2(x))
    }

    return top
  }
}
