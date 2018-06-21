# MongoDBWareManagement
Project simulating the management of a train transport company using MongoDB.

Our department is in charge of logistic associate to wares and their assignments  to wagons.

Every <b>Ware</b> is assigned to a <b>Wagon</b>, and has:
<ul>
  <li>Unique ID</li>
  <li>Ware tipology</li>
  <li>Shipping type</li>
  <li>Volumn</li>
  <li>Weight</li>
  <li>Shipping date</li>
  <li>Origin (We want to save Location and Geolocation)</li>
  <li>Destination (We want to save Location and Geolocation)</li>
</ul>

What we know about our <b>Clients</b>:
<ul>
  <li>Unique ID</li>
  <li>Name</li>
</ul>

About the <b>Wagon</b>:
<ul>
  <li>Unique ID</li>
  <li>Maximun Weight</li>
  <li>Maximun Volumn</li>
</ul>

Since we know that maybe in the future we can modify and increment variables on our database, we will include obligatory variables, and addmited varialbes in .txt.

In order to test this, we will perform some querys:
<ul>
  <li>1. List all wares of a client given.</li>
  <li>2. List every wares with origin and destination given.</li>
  <li>3. Calculate total weight and total volumn of a given client a given day. </li>
  <li>4. Calculate average density of all wares of a given client a given year.</li>
  <li>5. Shipping number for every month between an origin and a destination given.</li>
  <li>6. 3 destinations which have received more shippings with their number, for an origin and year given.</li>
  <li>7. Wares within a 100km circle distance of a destination given, ordered by distance.</li>
  <li>8. All wares within a wagon.</li>
  <li>9. All kind of wares existent in a wagon and how many of each one.</li>  
</ul>
