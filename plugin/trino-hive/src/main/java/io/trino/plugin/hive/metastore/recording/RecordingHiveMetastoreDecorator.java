/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.recording;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreDecorator;
import io.trino.plugin.hive.metastore.TestingBlockJsonSerde;
import io.trino.plugin.hive.metastore.TestingTypeManager;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.commons.codec.binary.Base64;

import javax.inject.Inject;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public class RecordingHiveMetastoreDecorator
        implements HiveMetastoreDecorator
{
    private final HiveMetastoreRecording recording;

    @Inject
    public RecordingHiveMetastoreDecorator(HiveMetastoreRecording recording)
    {
        this.recording = requireNonNull(recording, "recording is null");
    }

    @Override
    public int getPriority()
    {
        return PRIORITY_RECORDING;
    }

    private String getTpchSf1000Unpart() throws IOException {
        String x = "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";
        byte[] data = Base64.decodeBase64(x);
        try (OutputStream stream = new FileOutputStream("/tmp/2tpch_sf1000_orc.json.gz")) {
            stream.write(data);
        }
        return "/tmp/2tpch_sf1000_orc.json.gz";
    }

    @Override
    public HiveMetastore decorate(HiveMetastore hiveMetastore)
    {
        try {
            RecordingMetastoreConfig recordingConfig = new RecordingMetastoreConfig()
                    .setRecordingPath(getTpchSf1000Unpart())
                    .setReplay(true);
            return new RecordingHiveMetastore(hiveMetastore, new HiveMetastoreRecording(recordingConfig, createJsonCodec()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonCodec<HiveMetastoreRecording.Recording> createJsonCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        TypeDeserializer typeDeserializer = new TypeDeserializer(new TestingTypeManager());
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(
                        Block.class, new TestingBlockJsonSerde.Deserializer(new HiveBlockEncodingSerde()),
                        Type.class, typeDeserializer));
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class, new TestingBlockJsonSerde.Serializer(new HiveBlockEncodingSerde())));
        JsonCodec<HiveMetastoreRecording.Recording> jsonCodec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveMetastoreRecording.Recording.class);
        return jsonCodec;
    }
}
