<img src="./readme/title1.svg"/>

<br><br>

<!-- project philosophy -->
<img src="./readme/title2.svg"/>

> A mobile app for ordering coffee on-the-go, making it easier for coffee lovers to get their favorite beverages without waiting in line.
>
> Coffee Express aims to streamline the coffee-ordering process by providing a user-friendly platform for customers to place orders and pick up their coffee at their convenience. We believe in enhancing the coffee experience by saving time and ensuring customer satisfaction.

### User Stories
- As a user, I want to browse the menu, so I can find my favorite coffee drinks.
- As a user, I want to customize my order, so I can add or remove ingredients according to my preferences.
- As a user, I want to pay for my order through the app, so I can avoid waiting in line at the store.

<br><br>
<!-- Tech stack -->
<img src="./readme/title3.svg"/>

###  Coffee Express is built using the following technologies:

- This project uses the [Flutter app development framework](https://flutter.dev/). Flutter is a cross-platform hybrid app development platform which allows us to use a single codebase for apps on mobile, desktop, and the web.
- For persistent storage (database), the app uses the [Hive](https://hivedb.dev/) package which allows the app to create a custom storage schema and save it to a local database.
- To send local push notifications, the app uses the [flutter_local_notifications](https://pub.dev/packages/flutter_local_notifications) package which supports Android, iOS, and macOS.
  - 🚨 Currently, notifications aren't working on macOS. This is a known issue that we are working to resolve!
- The app uses the font ["Work Sans"](https://fonts.google.com/specimen/Work+Sans) as its main font, and the design of the app adheres to the material design guidelines.

<br><br>


<!-- Database Design -->
<img src="./readme/title5.svg"/>

###  Architecting Data Excellence: Innovative Database Design Strategies:

<img src="./readme/ER_diagram.jpg"/>


<br><br>


<!-- Implementation -->
<img src="./readme/title6.svg"/>


### User Screens (Power BI report)

| Landing Page                          | Map Overview                                |
| ----------------------------------------- | ----------------------------------------- |
| ![Demo](./readme/landing_page.png) | ![Demo](./readme/map_overview.png) |

| Country-Specific Drill-Through          | Sneak Peek                          |
| --------------------------------- | -------------------------------------- |
| ![Demo](./readme/country_drill_through.png) | ![Demo](./readme/sneek_peak.png) |

<!-- | Disease Analysis Screen             | Decomposition Tree Screen         |
| ----------------------------------- | --------------------------------- |
| ![Demo](./readme/assets/da_1_1.gif) | ![Demo](./readme/assets/dt_1.gif) | -->

<br><br>


<!-- Prompt Engineering -->
<img src="./readme/title7.svg"/>

###  Mastering AI Interaction: Unveiling the Power of Prompt Engineering:

- This project uses advanced prompt engineering techniques to optimize the interaction with natural language processing models. By skillfully crafting input instructions, we tailor the behavior of the models to achieve precise and efficient language understanding and generation for various tasks and preferences.

<br><br>

<!-- AWS Deployment -->
<img src="./readme/title8.svg"/>

###  Efficient AI Deployment: Unleashing the Potential with AWS Integration:

- This project leverages AWS deployment strategies to seamlessly integrate and deploy natural language processing models. With a focus on scalability, reliability, and performance, we ensure that AI applications powered by these models deliver robust and responsive solutions for diverse use cases.

<br><br>

<!-- Unit Testing -->
<img src="./readme/title9.svg"/>

###  Precision in Development: Harnessing the Power of Unit Testing:

- This project employs rigorous unit testing methodologies to ensure the reliability and accuracy of code components. By systematically evaluating individual units of the software, we guarantee a robust foundation, identifying and addressing potential issues early in the development process.

<br><br>


<!-- How to run -->
<img src="./readme/title10.svg"/>

> To set up Coffee Express locally, follow these steps:

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.
* npm
  ```sh
  npm install npm@latest -g
  ```

### Installation

_Below is an example of how you can instruct your audience on installing and setting up your app. This template doesn't rely on any external dependencies or services._

1. Get a free API Key at [example](https://example.com)
2. Clone the repo
   git clone [github](https://github.com/your_username_/Project-Name.git)
3. Install NPM packages
   ```sh
   npm install
   ```
4. Enter your API in `config.js`
   ```js
   const API_KEY = 'ENTER YOUR API';
   ```

Now, you should be able to run Coffee Express locally and explore its features.
