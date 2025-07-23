(() => {
    // Task queue system for managing asynchronous operations.
    const taskQueue = {
        pendingTasks: [],
        isProcessing: false,

        enqueue(task) {
            this.pendingTasks.push(() =>
                Promise.resolve(task()).catch((error) => {
                    console.error("Task execution error:", error);
                    return Promise.reject(error);
                })
            );
            if (!this.isProcessing) {
                this.processQueue();
            }
        },

        async processQueue() {
            this.isProcessing = true;
            while (this.pendingTasks.length > 0) {
                const currentTask = this.pendingTasks.shift();
                try {
                    await currentTask();
                } catch (error) {
                    console.error("Task execution error:", error);
                }
            }
            this.isProcessing = false;
        }
    };

    // DOM Element References.
    const eventSource = new EventSource("/events");
    const loadMoreButton = document.getElementById("load-more");
    const buttonTextElement = loadMoreButton.querySelector(".button-text");
    const itemsContainer = document.getElementById("items");
    const metadataContainer = document.querySelector(".metadata");

    // Animation Configuration.
    const textFadeDuration = parseInt(loadMoreButton.getAttribute("data-updating-button-text-duration"), 10);
    const signalAnimationDuration = parseInt(loadMoreButton.getAttribute("data-sending-signal-duration"), 10);
    const loadingText = loadMoreButton.getAttribute("data-sending-signal-text");

    // Set CSS animation properties.
    loadMoreButton.style.setProperty("--sending-signal-animation-duration", `${signalAnimationDuration / 1000}s`);
    loadMoreButton.style.setProperty("--updating-button-text-duration", `${textFadeDuration / 1000}s`);


    // Request more data from the server.
    const fetchMoreItems = () => {
        fetch("/load-more", { method: "GET" })
            .then((response) => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
            })
            .catch((error) => {
                console.error("Error fetching more items:", error);
            });
    };



    //////////////////////////////////////////////
    // CORE FUNCTIONS
    //////////////////////////////////////////////

    const setButtonTextElementOpacityCore = (opacity) => {
        if (opacity === 1) {
            loadMoreButton.classList.add("border-animated");

        } else {
            loadMoreButton.classList.remove("border-animated");

        }
        buttonTextElement.style.opacity = opacity;
    }

    const fadeButtonTextOutCore = () => {
        return new Promise((resolve) => {
            setButtonTextElementOpacityCore(0);
            setTimeout(resolve, textFadeDuration);
        });
    };

    const fadeButtonTextInCore = () => {
        return new Promise((resolve) => {
            setButtonTextElementOpacityCore(1);
            setTimeout(resolve, textFadeDuration);
        });
    };

    const updateButtonTextCore = (textFunction) => {
        buttonTextElement.innerHTML = textFunction();
    };

    const animateButtonTextChangeCore = (textFunction) => {
        const condition = () => {
            return buttonTextElement.innerHTML !== textFunction();
        };
        enqueueFadeButtonTextOut(condition);
        enqueueUpdateButtonText(textFunction, condition);
        enqueueFadeButtonTextIn(condition);
    }


    const disableLoadMoreButtonCore = () => {
        loadMoreButton.classList.add("disabled");
    };

    const appendNewItemsCore = (html) => {
        itemsContainer.innerHTML += html;
        return new Promise((resolve) => {
            requestAnimationFrame(() => {
                itemsContainer.scrollTo({
                    left: 0,
                    top: itemsContainer.scrollHeight,
                    behavior: "smooth"
                });
                // Wait briefly for the smooth scroll to complete.
                setTimeout(resolve, 100);
            });
        });
    };

    const refreshMetadataCore = (html) => {
        metadataContainer.innerHTML = html;
    };

    //////////////////////////////////////////////
    // ENQUEUE FUNCTIONS
    //////////////////////////////////////////////

    const enqueueFadeButtonTextOut = (condition) => {
        return taskQueue.enqueue(() => {
            if ((condition()) && buttonTextElement.style.opacity != 0) {
                return fadeButtonTextOutCore()
            }
        });
    };


    const enqueueFadeButtonTextIn = (condition) => {
        return taskQueue.enqueue(() => {
            if ((condition() || buttonTextElement.style.opacity == 0) /*&& button   TextElement.style.opacity != 1*/) {
                return fadeButtonTextInCore()
            }
        });
    };
    const enqueueUpdateButtonText = (textFunction, condition) => {
        return taskQueue.enqueue(() => {
            if (condition()) {
                return updateButtonTextCore(textFunction)
            }
        });
    };

    const enqueueUpdateButtonTextWithAnimation = (textFunction) => {
        return taskQueue.enqueue(() => animateButtonTextChangeCore(textFunction));
    };

    const enqueueDisableLoadMoreButton = () => {
        return taskQueue.enqueue(disableLoadMoreButtonCore);
    };



    // Handle server-sent events.
    eventSource.onmessage = (event) => {
        if (!event.data) return;
        const serverResponse = JSON.parse(event.data);

        if (serverResponse.templateInstructions) {
            const {
                maybeText,
                makeButtonDisabled,
                finishStream,
                setTextAsAlternativeDefaultButtonText
            } = serverResponse.templateInstructions;
            if (setTextAsAlternativeDefaultButtonText) {
                taskQueue.enqueue(() => {
                    customDefaultText = maybeText;
                })
            }
            if (maybeText) {
                enqueueUpdateButtonTextWithAnimation(() => maybeText);
            }
            if (makeButtonDisabled === true) {
                enqueueDisableLoadMoreButton();
            }
            if (finishStream === true) {
                taskQueue.enqueue(() => eventSource.close());
            }
        }

        if (serverResponse.userData) {
            appendNewItemsCore(serverResponse.userData);
        }

        if (serverResponse.userMetaData) {
            refreshMetadataCore(serverResponse.userMetaData); // not animated
        }
    };

    eventSource.onerror = () => {
        taskQueue.enqueue(() => {
            eventSource.close();
            loadMoreButton.classList.add("disabled");
            //  buttonTextElement.innerHTML = "Stream finished";
        });
    };

    // Load More Button click handler.
    loadMoreButton.onclick = () => {
        if (
            loadMoreButton.classList.contains("disabled") ||
            loadMoreButton.classList.contains("sending-signal")
        ) {
            return;
        }

        enqueueFadeButtonTextOut(() => true);
        taskQueue.enqueue(() => {
            loadMoreButton.classList.add("sending-signal");
        });
        enqueueUpdateButtonText(() => loadingText, () => true);
        enqueueFadeButtonTextIn(() => true);

        taskQueue.enqueue(() => {
            return new Promise((resolve) => {
                fetchMoreItems();
                setTimeout(() => {
                    loadMoreButton.classList.remove("sending-signal");
                    setButtonTextElementOpacityCore(0);
                    resolve();
                }, Math.max(0, signalAnimationDuration - textFadeDuration));
            });
        });

        taskQueue.enqueue(() => {
            return new Promise((resolve) => {
                setTimeout(resolve, textFadeDuration);
            });
        });

    };

})();