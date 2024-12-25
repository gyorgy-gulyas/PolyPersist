mkdir contracts
echo. > contracts\.gitkeep

mkdir generated
cd generated
mkdir dotnet
echo. > dotnet\.gitkeep
mkdir java
echo. > java\.gitkeep
mkdir python
echo. > python\.gitkeep
cd ..

mkdir implementations
cd implementations
mkdir dotnet
echo. > dotnet\.gitkeep
mkdir java
echo. > java\.gitkeep
mkdir python
echo. > python\.gitkeep
cd ..

mkdir tests
cd tests
mkdir dotnet
echo. > dotnet\.gitkeep
mkdir java
echo. > java\.gitkeep
mkdir python
echo. > python\.gitkeep
cd ..

mkdir tools
echo. > tools\.gitkeep

REM Visszalépés az alap mappába
cd ..
echo Mappák létrehozva!