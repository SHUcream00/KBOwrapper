# KBOwrapper
![License](https://img.shields.io/github/license/SHUcream00/KBOwrapper.svg)
![Forks](https://img.shields.io/github/forks/SHUcream00/KBOwrapper.svg)
![Stars](https://img.shields.io/github/stars/SHUcream00/KBOwrapper.svg)
![Watchers](https://img.shields.io/github/watchers/SHUcream00/KBOwrapper.svg)
[![LinkedIn][linkedin-shield]][linkedin-url]

<div align="center">
  <a href="https://github.com/SHUcream00/KBOwrapper">
    <img src="images/mpi.gif">
  </a>

  <h3 align="center">KBOwrapper</h3>

</div>

This project helps users to [Message Passing Interface](https://en.wikipedia.org/wiki/Message_Passing_Interface) to create a distributed program that creates and outputs the transpose of the original matrix. There will be only one process with rank 0, that will read the file name with the input data, read from the file the values of n and m, read from the file the entire matrix, and will output the resulting matrix on the screen. All the other processes will receive portions of the matrix and contribute to creating the transposed matrix.

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

![Sample_screenshot][example_ss]

There are many ways to build a distributed, scalable program to achieve high performance. 
This project is built as demonstration of one of such parallel programming models using [MSMPI](https://docs.microsoft.com/en-us/message-passing-interface/microsoft-mpi), to distribute mathematical operations on several threads to improve scalabilty, processing speed.

This program can be further modified to comply with other requirements.

With this program, you can:
* Transpose your beautiful matrix elegantly with parallel process.

<p align="right">(<a href="#top">back to top</a>)</p>



### Built With

* [Python](https://www.python.org/)
* [mpi4py](https://mpi4py.readthedocs.io/en/stable/)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

This is a simple code with only dependency being [mpi4py](https://mpi4py.readthedocs.io/en/stable/)

### Prerequisites

MPI_Matrix_Transpose requires [mpi4py](https://mpi4py.readthedocs.io/en/stable/) library to run. 
You can easily install the library through pip.
* mpi4py
  ```sh
  pip -install mpi4py
  ```

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/SHUcream00/MPI_Matrix_Transpose
   ```

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage

![usage_gather][example_ss2]
![usage_gather][example_ss3]

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- ROADMAP -->
## Roadmap

- [x] Implement MPI
- [x] Finalize

This project is final.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

This project is final and not being maintained. You can fork this repository and keep going on.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

[![LinkedIn][linkedin-shield]][linkedin-url]

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

I send my thanks to the authors of the following resources for helping me get this project get better.

* [Othneil Drew's Readme Template](https://github.com/othneildrew/Best-README-Template)

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[linkedin-shield]: https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white
[linkedin-url]: https://www.linkedin.com/in/joon-won-choi
[example_ss]: images/example.png
[example_ss2]: images/MPI_Gather.gif
[example_ss3]: images/MPI_Scatter.gif

