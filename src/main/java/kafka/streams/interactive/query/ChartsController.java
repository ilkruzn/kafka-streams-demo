package kafka.streams.interactive.query;

import static kafka.streams.interactive.query.KafkaStreamsInteractiveQuerySample.KEY_TOP_FIVE;
import static kafka.streams.interactive.query.KafkaStreamsInteractiveQuerySample.STORE_ALL_SONGS;
import static kafka.streams.interactive.query.KafkaStreamsInteractiveQuerySample.STORE_TOP_FIVE_SONGS;

import java.util.ArrayList;
import java.util.List;
import kafka.streams.interactive.query.avro.Song;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class ChartsController {

  @Autowired
  private InteractiveQueryService interactiveQueryService;

  private final Log logger = LogFactory.getLog(getClass());

  @GetMapping("/charts/top-five")
  @SuppressWarnings("unchecked")
  public List<SongPlayCountBean> topFive() {

    HostInfo hostInfo = interactiveQueryService.getHostInfo(STORE_TOP_FIVE_SONGS,
        KEY_TOP_FIVE, new StringSerializer());

    if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
      logger.info("Top Five songs request served from same host: " + hostInfo);
      return topFiveSongs(KEY_TOP_FIVE, STORE_TOP_FIVE_SONGS);
    } else {
      //find the store from the proper instance.
      logger.info("Top Five songs request served from different host: " + hostInfo);
      RestTemplate restTemplate = new RestTemplate();
      return restTemplate.getForObject(
          String.format("http://%s:%s/charts/top-five",
              hostInfo.host(),
              hostInfo.port()),
          List.class);
    }
  }

  @GetMapping("/songs/{id}")
  public SongBean song(@PathVariable Long id) {
    final ReadOnlyKeyValueStore<Long, Song> songStore =
        interactiveQueryService
            .getQueryableStore(STORE_ALL_SONGS,
                QueryableStoreTypes.<Long, Song>keyValueStore());

    final Song song = songStore.get(id);
    if (song == null) {
      throw new IllegalArgumentException("Song not found");
    }
    return new SongBean(song.getId(), song.getArtist(), song.getAlbum(), song.getName());
  }

  private List<SongPlayCountBean> topFiveSongs(final String key, final String storeName) {

    final ReadOnlyKeyValueStore<String, TopFiveSongs> topFiveStore =
        interactiveQueryService.getQueryableStore(storeName, QueryableStoreTypes.keyValueStore());

    // Get the value from the store
    final TopFiveSongs value = topFiveStore.get(key);
    if (value == null) {
      throw new IllegalArgumentException(
          String.format("Unable to find value in %s for key %s", storeName, key));
    }
    final List<SongPlayCountBean> results = new ArrayList<>();
    value.forEach(songPlayCount -> {

      HostInfo hostInfo = interactiveQueryService
          .getHostInfo(STORE_ALL_SONGS, songPlayCount.getSongId(), new LongSerializer());

      if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {

        final ReadOnlyKeyValueStore<Long, Song> songStore =
            interactiveQueryService
                .getQueryableStore(STORE_ALL_SONGS,
                    QueryableStoreTypes.<Long, Song>keyValueStore());

        final Song song = songStore.get(songPlayCount.getSongId());
        logger.info(
            String.format("Song info %d request served from same host: %s", song.getId(), hostInfo));
        results.add(
            new SongPlayCountBean(songPlayCount.getSongId(), song.getArtist(), song.getAlbum(),
                song.getName(),
                songPlayCount.getPlays()));
      } else {
        logger.info(String.format("Song info %d request served from different host: %s",
            songPlayCount.getSongId(), hostInfo));

        RestTemplate restTemplate = new RestTemplate();

        SongBean song = restTemplate.getForObject(
            String.format("http://%s:%d/songs/{id}",
                hostInfo.host(),
                hostInfo.port()),
            SongBean.class,
            songPlayCount.getSongId());

        results.add(
            new SongPlayCountBean(songPlayCount.getSongId(), song.getArtist(), song.getAlbum(),
                song.getName(),
                songPlayCount.getPlays()));
      }


    });
    return results;
  }
}
