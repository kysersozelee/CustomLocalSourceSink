/*
 * Copyright [2017] [kysersoze.lee@gmail.com]
 *
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

package com.github.kysersozelee.Source;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.*;
import org.apache.flume.source.AbstractEventDrivenSource;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.HashMap;
import java.util.Objects;


@Slf4j
public class SubSource extends AbstractEventDrivenSource implements EventDrivenSource {
    private static final HashMap<Integer, Subscriber<Event>> subscriberPerIdx = Maps.newHashMap();

    //사용할 subSourceIdx. 설정 값에 의해 정의된 SubSource의 고유 idx. Subscription시에 해당 값으로 어느 소스에서 이벤트를 처리할지 결정.
    private static final String CONFIG_SUB_SOURCE_IDX = "subSourceIdx";
    //모든 subSourceIdx 값들. 해당 값들마다 고유한 Subscriber를 생성
    private static final String CONFIG_DEST_SUB_SOURCE_IDX_LIST = "destSubSourceIdxList";

    private int currentSubSourceIdx = 0;

    public SubSource() {
        super();
    }


    protected void doConfigure(Context context) throws FlumeException {
        super.configure(context);
        currentSubSourceIdx = Objects.requireNonNull(context.getInteger(CONFIG_SUB_SOURCE_IDX));
        String subSourceIdxList = Objects.requireNonNull(context.getString(CONFIG_DEST_SUB_SOURCE_IDX_LIST));
        String[] destSubSourceIdxList = subSourceIdxList.replace(" ", "").split(",");

        //SubSource 개수만큼 Subscriber를 생성
        for (String destSubSourceIdxStr : destSubSourceIdxList) {
            int destSubSourceIdx = Integer.parseInt(destSubSourceIdxStr);

            subscriberPerIdx.putIfAbsent(destSubSourceIdx, new Subscriber<Event>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Event event) {
                    //
                    if (destSubSourceIdx == currentSubSourceIdx) {
                        try {
                            getChannelProcessor().processEvent(event);
                        } catch (ChannelException e) {
                            onError(e);
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.debug("onError. error:{}", t.getMessage());
                }

                @Override
                public void onComplete() {
                    log.debug("onColplete");
                }
            });
            log.info("Success to add subscriber. currentSubSourceIdx{}, destSubSourceIdx:{}, subscriber:{}", new Object[]{currentSubSourceIdx, destSubSourceIdxList, subscriberPerIdx});
        }
    }

    protected void doStart() throws FlumeException {
        super.start();
    }

    protected void doStop() throws FlumeException {
        super.stop();
    }
}
