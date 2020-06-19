package kafka.streams.interactive.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import kafka.streams.interactive.query.avro.SongPlayCount;

/**
 * Used in aggregations to keep track of the Top five songs
 */
class TopFiveSongs implements Iterable<SongPlayCount> {

  private final Map<Long, SongPlayCount> currentSongs = new ConcurrentHashMap<>();
  private final SortedSet<SongPlayCount> topFive = Collections
      .synchronizedSortedSet(new TreeSet<>((o1, o2) -> {
        final int result = o2.getPlays().compareTo(o1.getPlays());
        if (result != 0) {
          return result;
        }
        return o1.getSongId().compareTo(o2.getSongId());
      }));

  public void add(final SongPlayCount songPlayCount) {
    topFive.add(songPlayCount);
    if (topFive.size() > 5) {
      topFive.remove(topFive.last());
    }
//    if (currentSongs.containsKey(songPlayCount.getSongId())) {
//      topFive.remove(currentSongs.remove(songPlayCount.getSongId()));
//    }
//    topFive.add(songPlayCount);
//    currentSongs.put(songPlayCount.getSongId(), songPlayCount);
//    if (topFive.size() > 5) {
//      final SongPlayCount last = topFive.last();
//      currentSongs.remove(last.getSongId());
//      topFive.remove(last);
//    }
  }

  void remove(final SongPlayCount value) {
    topFive.remove(value);
//    currentSongs.remove(value.getSongId());
  }


  @Override
  public Iterator<SongPlayCount> iterator() {
    return topFive.iterator();
  }
}
