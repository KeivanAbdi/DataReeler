# DataReeler

<p align="center">
  <img src="DataReeler.webp" alt="DataReeler Logo" width="300"/>
</p>

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Contributions Welcome](https://img.shields.io/badge/Contributions-Welcome-brightgreen.svg)](CONTRIBUTING.md)

An interactive toolkit for prototyping, debugging, and interactively exploring Akka/Pekko streams, putting you in control of the data flow. `datareeler` is a specialized developer tool that sits between your Akka/Pekko stream logic and a web browser.

## 📖 Table of Contents

- [✨ Features](#-features)
- [💡 Core Concepts](#-core-concepts)
- [🚀 Example Use Cases](#-example-use-cases)
- [📚 Minimal Example](#-minimal-example)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

## ✨ Features

- **Signal-Controlled Streams**: Go beyond simple back-pressure with explicit, signal-driven control to pause and resume your streams directly from the UI.
- **Composable UI Traits**: Decouple stream logic from presentation. Implement the `ReelerTemplate` trait to create self-contained, reusable UI modules with their own assets.
- **Customizable Default Template**: Comes with a default `FullWidthInfiniteScroll` template that can be customized by injecting CSS and Javascript, or by adjusting its parameters. You can also create your own template from scratch.
- **Tri-Component Stream Events**: A single `ReelElement` can carry your core `Data`, contextual `MetaData` (like logs or stats), and `TemplateInstruction`s for the UI, all in one elegant package.
- **Feedback-Driven Streams**: Inject custom Pekko HTTP routes to send signals from your UI back to your stream logic, enabling dynamic, adaptive stream behavior.
- **Zero-Configuration Web Server**: Get up and running instantly with a pre-configured Pekko HTTP server that handles SSE connections and data loading endpoints out of the box.

## 💡 Core Concepts

- **Interactive Exploration over Batch Processing**: We believe that developers should be able to "converse" with their data streams. Instead of waiting for a massive batch job to finish, `datareeler` allows you to pull data on-demand, inspect the results in real-time, and immediately adjust your approach.
- **Developer in Control of the Flow**: The library is built on the idea that the end-user, the developer, should be the ultimate arbiter of back-pressure. The UI is not just a passive viewer; it's the primary driver of demand in the system.
- **Decoupled Logic, Pluggable UI**: The stream processing logic should be entirely separate from its presentation. `datareeler` enforces this by design, allowing you to swap out complex UIs without ever touching your core data-fetching code.

## 🚀 Example Use Cases

`datareeler` excels at building "human-in-the-loop" applications where a user can interactively explore, filter, and enrich a large or continuous stream of data. The following are just a couple of examples to illustrate the possibilities.

### Curated Social Media Feed
-   **Scenario:** Create a personalized newsfeed from a source like the X (formerly Twitter) API. The goal is to display posts based on a search term, but only from users who align with a specific viewpoint.
-   **Implementation:**
    1.  **Lazy Stream:** The `inputReelElementStream` connects to the X API as a lazy `Source`, pulling tweets on-demand as the user clicks "Load More".
    2.  **Real-time Enrichment:** As each tweet flows through the stream, a secondary call to another service can check the author's affiliation.
    3.  **Interactive Filtering:** The UI displays only the qualifying tweets. Using the `userRoutes` feedback mechanism, the user could submit a new list of "approved" authors, which an Actor on the backend uses to modify the filtering logic for all subsequent tweets.

### Targeted Job Application Assistant
-   **Scenario:** Build a tool to streamline the job search process on a platform like LinkedIn. The goal is to pull a stream of job postings from a search query and allow a user to deeply inspect, cross-reference, and handpick the most relevant ones.
-   **Implementation:**
    1.  **Lazy Stream:** The `inputReelElementStream` would perform a search on LinkedIn, lazily pulling job postings one by one.
    2.  **Deep Inspection & Enrichment:** For each job posting, the stream could perform several enrichment steps in parallel, such as cross-referencing the company with Glassdoor for reviews or analyzing the job description for key skills.
    3.  **Hand-picking & Action:** The UI would display each job posting along with all the enriched data (`userMetaData`). The user can then inspect the results and, using the `userRoutes` feedback loop, click a "Save for Later" or "Not Interested" button to build a curated list.

## 📚 Minimal Example

This example showcases the bare minimum required to get the system up and running. More comprehensive, real-world examples will be published in the future.

First, add the dependency to your `build.sbt` file:
```scala
libraryDependencies += "com.keivanabdi" %% "datareeler" % "0.1.0"
```

Then, you can use it in your code:
```scala
import com.keivanabdi.datareeler.models.*
import com.keivanabdi.datareeler.system.ReelerSystem
import com.keivanabdi.datareeler.templates.FullWidthInfiniteScroll
import com.keivanabdi.datareeler.templates.FullWidthInfiniteScroll.Instructions
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import scala.concurrent.duration.*
import scalatags.Text.all.*

implicit val system: ActorSystem = ActorSystem("ReelerSystem")

// Define your data and metadata types
case class MyData(id: Int, name: String)
case class MyMetaData(processedCount: Int)

// 1. Define your input stream. Please note: this should be a lazy source to respect back-pressure signals.
def myInputStream()
    : Source[ReelElement[MyData, MyMetaData, Instructions], ?] = {
  Source(1 to 20)
    .map { i =>
      ReelElement[MyData, MyMetaData, Instructions](userData =
        Some(MyData(i, s"Item #$i"))
      )
    }
    .statefulMapConcat { () =>
      var processedCount = 0
      { reelElement =>
        processedCount += 1
        Iterator.single(
          reelElement
            .copy(userMetaData =
              Some(MyMetaData(processedCount = processedCount))
            )
        )
      }
    }
    .throttle(3, 1.second)
}

// 2. Define the HTML template and its renderers
val reelerTemplate = FullWidthInfiniteScroll[MyData, MyMetaData](
  dataHtmlRenderer = { _ => data =>
    HtmlRenderable.ScalatagsHtml(div(h2(data.name), p(s"ID: ${data.id}")))
  },
  metaHtmlRenderer = { _ => meta =>
    HtmlRenderable.ScalatagsHtml(
      i(s"Processed: ${meta.processedCount} items.")
    )
  },
  styleBlocks                              = Seq.empty,
  javascriptBlocks                         = Seq.empty,
  defaultButtonText                        = "Load More",
  previouslyRequestedItemsProcessedText    = "Loaded. Click to load more...",
  previouslyRequestedItemsNotProcessedText = "Still processing...",
  streamFinishedText                       = "All items loaded",
  sendingSignalText                        = "Requesting..."
)

// 3. Create the main configuration object
val reelerConfig = ReelerSystemConfig(
  reelerTemplate  = reelerTemplate,
  initialMetaData = () => MyMetaData(processedCount = 0),
  demandBatchSize = 10,
  timeout         = 30.seconds
)

// 4. Instantiate and start the ReelerSystem
val reelerSystem =
  ReelerSystem[MyData, MyMetaData, FullWidthInfiniteScroll.Instructions](
    inputReelElementStream = () => myInputStream(),
    config                 = reelerConfig,
    userRoutes             = Seq.empty
  )

val bindingFuture = reelerSystem.start()
```

## 🤝 Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
