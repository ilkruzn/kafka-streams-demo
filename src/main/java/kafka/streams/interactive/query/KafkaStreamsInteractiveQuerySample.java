/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.streams.interactive.query;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import kafka.streams.interactive.query.avro.PlayEvent;
import kafka.streams.interactive.query.avro.Song;
import kafka.streams.interactive.query.avro.SongPlayCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaStreamsInteractiveQuerySample {

  public static void main(String[] args) {
    SpringApplication.run(KafkaStreamsInteractiveQuerySample.class, args);
  }

  static final String KEY_TOP_FIVE = "all";
  static final String STORE_ALL_SONGS = "all-songs";
  private static final String STORE_SONG_PLAY_COUNT = "song-play-count";
  static final String STORE_TOP_FIVE_SONGS = "top-five-songs";

  private static final Long MIN_CHARTABLE_DURATION = 30 * 1000L;

  @Bean
  public BiConsumer<KStream<String, PlayEvent>, KTable<Long, Song>> process() {

    return (playEvents, songs) -> {
      // create and configure the SpecificAvroSerdes required in this example
      final Map<String, String> serdeConfig = Collections.singletonMap(
          AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

      final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
      playEventSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<Song> keySongSerde = new SpecificAvroSerde<>();
      keySongSerde.configure(serdeConfig, true);

      final SpecificAvroSerde<Song> valueSongSerde = new SpecificAvroSerde<>();
      valueSongSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<SongPlayCount> songPlayCountSerde = new SpecificAvroSerde<>();
      songPlayCountSerde.configure(serdeConfig, false);

      final TopFiveSerde topFiveSerde = new TopFiveSerde();

      // Accept play events that have a duration >= the minimum
      final KStream<Long, PlayEvent> playsBySongId =
          playEvents.filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
              // repartition based on song id
              .map((key, value) -> KeyValue.pair(value.getSongId(), value));

      // join the plays with song as we will use it later for charting
      final KStream<Long, Song> songPlays = playsBySongId.leftJoin(songs,
          (value1, song) -> song,
          Joined.with(Serdes.Long(), playEventSerde, valueSongSerde));

      // create a state store to track song play counts
      final KTable<Song, Long> songPlayCounts = songPlays.groupBy((songId, song) -> song,
          Serialized.with(keySongSerde, valueSongSerde))
      .count();

      songPlayCounts.groupBy((song, plays) ->
              KeyValue.pair(KEY_TOP_FIVE,
                  new SongPlayCount(song.getId(), plays)),
          Serialized.with(Serdes.String(), songPlayCountSerde))
          .aggregate(TopFiveSongs::new,
              (aggKey, value, aggregate) -> {
                aggregate.add(value);
                return aggregate;
              },
              (aggKey, value, aggregate) -> {
                aggregate.remove(value);
                return aggregate;
              },
              Materialized.<String, TopFiveSongs, KeyValueStore<Bytes, byte[]>>as(
                  STORE_TOP_FIVE_SONGS)
                  .withKeySerde(Serdes.String())
                  .withValueSerde(topFiveSerde)
          );
    };

  }

}
